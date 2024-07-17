package com.lightbend.uprotocol

import com.google.protobuf.empty.Empty
import com.google.protobuf.wrappers.BytesValue
import com.lightbend.uprotocol.cloudevent.factory.KalixUCloudEvent
import com.lightbend.uprotocol.model.Implicits._
import com.lightbend.uprotocol.model.common._
import io.cloudevents.protobuf.{ProtobufFormat, WormholeProtoSerializer}
import kalix.scalasdk.DeferredCall
import kalix.scalasdk.action.Action
import com.google.protobuf.ByteString
import io.cloudevents.jackson.JsonFormat
import kalix.scalasdk.action.Action.Effect
import org.eclipse.{uprotocol => eclipseUprotocol}
import org.slf4j.LoggerFactory

import java.net.URI
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait BaseDispatcherAction extends Action {
  private def components: Components = new ComponentsImpl(contextForComponents)

  def in(bytesValue: BytesValue): Effect[Empty] = {
    import model.common._
//    log.info(s"input message: ${new String(bytesValue.value.toByteArray,StandardCharsets.UTF_8)}")
    val outerCloudEvent = ceProtoFormat.deserialize(bytesValue.value.toByteArray)

    val eclipseUMessage = KalixUCloudEvent.toMessage(outerCloudEvent)
    val uMessage = eclipseUMessage.toKalix

    uMessage.attributes.zip(uMessage.attributes.flatMap(_.source))
      .fold(effects.ignore[Empty]) { case (attributes, source) =>
        val eclipseAttributes = eclipseUMessage.getAttributes
        lazy val stringSource = uriSerializer.serialize(eclipseAttributes.getSource)

        attributes.msgType match {
          case UMessageType.UMESSAGE_TYPE_PUBLISH =>
            val topic = stringSource
//            log.info("Handling publish message for topic [{}]", topic)

            // Fetch the subscribers for the topic, forward the message to the subscribers
            def fanout(startingOffset: Option[Int]): Future[Unit] = {
              val fetchRequest = FetchRequest(topic, startingOffset)

              components.topic.fetchSubscribers(fetchRequest).execute()
                .flatMap { fetchResponse =>
                  val FetchResponse(subscribers, nextOffset, _) = fetchResponse

                  if (subscribers.nonEmpty) {
                    // fan-out to the subscribers...
                    val publishes =
                      fetchResponse.items.iterator.flatMap { item =>
                        item.subscriber.map { subscriberInfo =>
                          subscriberInfo.uri.map { uri =>
                            (PublishUMessage(Some(uMessage), Some(uri)),item.outputId)
                          }
                        }
                      }
                      .map (_.map( tuple => sendToUEntityOut(tuple._1,tuple._2).execute().flatMap(_ => Future.unit)))
                      .reduce { (a, b) => a.flatMap(_ => b) }

                    // ..and get the next batch (logically recursive, but the trampoline in the EC prevents
                    // StackOverflow)
                    val nextBatch = if (nextOffset.isDefined) fanout(nextOffset) else Future.unit
                    publishes.map(_ => nextBatch).getOrElse(Future.unit)
                  } else Future.unit
                }
            }

            effects.asyncEffect(
              fanout(None).map(_ => effects.reply(empty))
                .recover {
                  case ex => effects.error(s"Failed to fan-out message to subscribers. Reason: ${ex.getMessage}")
                }
            )

          case UMessageType.UMESSAGE_TYPE_UNSPECIFIED =>
            log.warn("Ignoring message with unspecified type.  Source was [{}]", stringSource)
            effects.ignore

          case UMessageType.UMESSAGE_TYPE_REQUEST =>
            // Forward the message with the sink as the envelope-CE subject
            // Requests for subscription will be handled without forwarding
            attributes.sink match {
              case None =>
                log.warn("No sink specified for req.v1 message.  Source was [{}]", stringSource)
                effects.ignore

              case Some(sink) =>
//                log.info(s"Sink: ${sink.entity}, resource: ${sink.resource.map(_.name)}/${sink.resource.map(_.instance)}")

                sink.entity match {
                  case None =>
                    log.warn("No sink.entity specified for req.v1 message.  Sink was [{}]", sink)
                    effects.ignore
                  case Some(entity) =>
                    if (entity.name == "core.usubscription" /* && sink.authority.isEmpty*/ ) {
                      //TODO Levi why sink.authority.isEmpty??
                      sink.resource.filter(_.name.startsWith("rpc"))
                        .fold {
                          log.warn("Sink resource did not start with RPC.  Sink was [{}]", sink)
                          effects.ignore[Empty]
                        } { resource =>
                          resource.instance match {
                            case Some("Subscribe") =>
                              uMessage.payload.map(_.data) match {
                                case None =>
                                  log.warn("No payload for Subscribe RPC.  Sink was [{}]", sink)
                                  effects.ignore

                                case Some(UPayload.Data.Value(bytes)) =>
                                  Try {
                                    val subscribeRequest =
                                      eclipseUprotocol.core.usubscription.v3.SubscriptionRequest.parseFrom(bytes)

                                    if (subscribeRequest.hasTopic) {
                                      val topic = uriSerializer.serialize(subscribeRequest.getTopic)

                                      val subscriber =
                                        if (subscribeRequest.hasSubscriber) Some(subscribeRequest.getSubscriber.toKalix)
                                        else None

                                      val subscriptionAttrs =
                                        if (subscribeRequest.hasAttributes) Some(subscribeRequest.getAttributes.toKalix)
                                        else None

                                      subscriber.map(_ => (topic, subscriber, subscriptionAttrs))
                                    } else None
                                  } match {
                                    case Failure(ex) =>
                                      log.warn("Exception thrown while processing Subscribe rpc.  Source was [{}]: {}", stringSource, ex)
                                      effects.ignore

                                    case Success(None) =>
                                      log.warn("Subscribe rpc requires topic and subscriber.  Source was [{}]", stringSource)
                                      effects.ignore

                                    case Success(Some(req)) =>
                                      // 1. Fetch the OutputId from the uEntityMetadata
                                      // 2. Subscribe to the Topic
                                      // 3. Publish Message to the the OutputId topic
                                      doStuff {
                                        req._2.flatMap(_.uri.flatMap(_.entity.map(_.name))) match {
                                          case Some(subscriberEntityName) => {
//                                            log.info("subscriberEntityName:{}", subscriberEntityName)
                                            components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                              .flatMap { entityMetadataResponse =>
                                                components.topic.subscribe(RequestToSubscribe(req._1, req._2, req._3, entityMetadataResponse.outputId)).execute()
                                                  .flatMap { subscribeResponse =>
                                                    val responseMsg = responseMessage(subscribeResponse.toByteString, attributes)
                                                    sendToUEntityOut(
                                                      PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                                    ).execute()
                                                  }
                                              }
                                          }
                                          case None =>
                                            log.warn("Failed to extract the EntityName of the Source.")
                                            Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                        }
                                      }
                                  }

                                case _ =>
                                  log.warn("Only value format is supported in Subscribe RPC.  Source was [{}]", stringSource)
                                  effects.ignore
                              }

                            case Some("Unsubscribe") =>
                              log.warn("Unsubscribe not implemented yet.  Source was [{}]", stringSource)
                              val responseMsg = responseMessage(notImplementedUStatus("Unsubscribe").toByteString, attributes)

                              doStuff {
                                responseMsg.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                                  case Some(subscriberEntityName) =>
                                    components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                      .flatMap { entityMetadataResponse =>
                                        sendToUEntityOut(
                                          PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                        ).execute()
                                      }
                                  case None =>
                                    log.warn("Failed to extract the EntityName of the Source.")
                                    Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                }
                              }

                            case Some("FetchSubscriptions") =>
                              log.warn("FetchSubscriptions uProtocol RPC not implemented yet.  Source was [{}]", stringSource)

                              val responseBytes =
                                eclipseUprotocol.core.usubscription.v3.FetchSubscriptionsResponse.newBuilder()
                                  .setStatus(notImplementedUStatus("FetchSubscriptions"))
                                  .build().toByteString

                              val responseMsg = responseMessage(responseBytes, attributes)

                              doStuff {
                                sendToUEntityOut(
                                  PublishUMessage(Some(responseMsg), attributes.source), 1
                                ).execute()
                              }

                            case Some("RegisterForNotifications") =>
                              log.warn("RegisterForNotifications uProtocol RPC not implemented yet.  Source was [{}]", stringSource)

                              val responseBytes = notImplementedUStatus("RegisterForNotifications").toByteString
                              val responseMsg = responseMessage(responseBytes, attributes)

                              doStuff {
                                responseMsg.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                                  case Some(subscriberEntityName) =>
                                    components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                      .flatMap { entityMetadataResponse =>
                                        sendToUEntityOut(
                                          PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                        ).execute()
                                      }
                                  case None =>
                                    log.warn("Failed to extract the EntityName of the Source.")
                                    Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                }
                              }

                            case Some("UnregisterForNotifications") =>
                              log.warn("UnregisterForNotifications uProtocol RPC not implemented yet.  Source was [{}]", stringSource)

                              val responseBytes = notImplementedUStatus("UnregisterForNotifications").toByteString
                              val responseMsg = responseMessage(responseBytes, attributes)

                              doStuff {
                                responseMsg.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                                  case Some(subscriberEntityName) =>
                                    components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                      .flatMap { entityMetadataResponse =>
                                        sendToUEntityOut(
                                          PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                        ).execute()
                                      }
                                  case None =>
                                    log.warn("Failed to extract the EntityName of the Source.")
                                    Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                }
                              }

                            case Some("FetchSubscribers") =>
                              log.warn("FetchSubscribers uProtocol RPC not implemented yet.  Source was [{}]", stringSource)

                              val responseBytes =
                                eclipseUprotocol.core.usubscription.v3.FetchSubscribersResponse.newBuilder()
                                  .setStatus(notImplementedUStatus("FetchSubscribers"))
                                  .build().toByteString

                              val responseMsg = responseMessage(responseBytes, attributes)

                              doStuff {
                                responseMsg.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                                  case Some(subscriberEntityName) =>
                                    components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                      .flatMap { entityMetadataResponse =>
                                        sendToUEntityOut(
                                          PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                        ).execute()
                                      }
                                  case None =>
                                    log.warn("Failed to extract the EntityName of the Source.")
                                    Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                }
                              }

                            case Some("Reset") =>
                              log.warn("Reset uProtocol RPC not implemented yet.  Source was [{}]", stringSource)

                              val responseBytes = notImplementedUStatus("Reset").toByteString
                              val responseMsg = responseMessage(responseBytes, attributes)

                              doStuff {
                                responseMsg.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                                  case Some(subscriberEntityName) =>
                                    components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                      .flatMap { entityMetadataResponse =>
                                        sendToUEntityOut(
                                          PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                        ).execute()
                                      }
                                  case None =>
                                    log.warn("Failed to extract the EntityName of the Source.")
                                    Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                }
                              }

                            case Some(_) =>
                              log.warn("Unknown RPC not implemented yet.  Source was [{}]", stringSource)

                              val responseBytes = notImplementedUStatus("Unknown").toByteString
                              val responseMsg = responseMessage(responseBytes, attributes)

                              doStuff {
                                responseMsg.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                                  case Some(subscriberEntityName) =>
                                    components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                                      .flatMap { entityMetadataResponse =>
                                        sendToUEntityOut(
                                          PublishUMessage(Some(responseMsg), attributes.source), entityMetadataResponse.outputId
                                        ).execute()
                                      }
                                  case None =>
                                    log.warn("Failed to extract the EntityName of the Source.")
                                    Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                                }
                              }
                            case _ =>
                              log.warn(
                                "Unexpected message component in resource: [{}] . Source was [{}]",
                                resource.message.getOrElse("[NOT SPECIFIED]"),
                                stringSource
                              )
                              effects.ignore
                          }
                        }

                    } else {
                      log.warn(s"RPC handling for software entity [${sink.entity}] is not implemented yet.")
                      effects.reply(empty)
                    }
                }
            }

          case UMessageType.UMESSAGE_TYPE_RESPONSE =>
            // Forward the message with the sink as the envelope-CE subject
            attributes.sink
              .fold(effects.ignore[Empty]) { recipient =>
                doStuff {
                  uMessage.attributes.flatMap(_.source.flatMap(_.entity.map(_.name))) match {
                    case Some(subscriberEntityName) =>
                      components.uEntityMetadata.fetchUEntityMetadata(FetchUEntityMetadataRequest(subscriberEntityName)).execute()
                        .flatMap { entityMetadataResponse =>
                          sendToUEntityOut(
                            PublishUMessage(Some(uMessage), attributes.source), entityMetadataResponse.outputId
                          ).execute()
                        }
                    case None =>
                      log.warn("Failed to extract the EntityName of the Source.")
                      Future.failed(new Exception("Failed to extract the EntityName of the Source."))
                  }
                }
              }
          case UMessageType.UMESSAGE_TYPE_NOTIFICATION =>
            log.warn("Ignoring message with unknown type [{}].  Source was [{}]", "UMESSAGE_TYPE_NOTIFICATION", stringSource)
            effects.ignore

          case UMessageType.Unrecognized(value) =>
            log.warn("Ignoring message with unknown type [{}].  Source was [{}]", value, stringSource)
            effects.ignore
        }
      }
  }

  def sendToUEntityOut(publishUMessage: PublishUMessage, outputId: Int): DeferredCall[_root_.com.lightbend.uprotocol.PublishUMessage, BytesValue]

  def sendToVehicleOut(publishUMessage: PublishUMessage): DeferredCall[_root_.com.lightbend.uprotocol.PublishUMessage, BytesValue]

  def out(publishUMessage: PublishUMessage): Effect[BytesValue] = {
    publishUMessage match {
      case PublishUMessage(Some(msg), Some(envelopeTo), _) =>
        msg.attributes.flatMap(_.source).map { u => uriSerializer.serialize(u.toEclipse) }
          .fold(effects.error[BytesValue]("No source specified")) { source =>
//            val innerEvent =
//              WormholeProtoSerializer.toProto(
//                eclipseUprotocol.cloudevent.factory.UCloudEvent.fromMessage(msg.toEclipse)
//              ).toKalix
            val innerEvent = eclipseUprotocol.cloudevent.factory.UCloudEvent.fromMessage(msg.toEclipse)
            val innerEventByteValue = BytesValue.of(ByteString.copyFrom(ceProtoFormat.serialize(innerEvent)))
            val outerId = java.util.UUID.randomUUID().toString
            
            var metadata =
              kalix.scalasdk.CloudEvent(outerId, URI.create(source), "kalix-uprotocol-v0")
                .withSubject(uriSerializer.serialize(envelopeTo.toEclipse))
                .asMetadata

            val attributes = msg.attributes
            attributes.flatMap(_.ttl).foreach { ttl => metadata = metadata.add("ttl", ttl.toString) }
            attributes.flatMap(_.token).foreach { token => metadata = metadata.add("token", token) }
            attributes.flatMap(_.permissionLevel).foreach { pl => metadata = metadata.add("plevel", pl.toString) }
            attributes.flatMap(_.traceparent).foreach { tp => metadata = metadata.add("traceparent", tp) }

            attributes.map(_.priority).filter(_.value > 0).foreach { p =>
              metadata =
                metadata.add("priority", eclipseUprotocol.cloudevent.factory.UCloudEvent.getCePriority(p.toEclipse))
            }

            attributes.flatMap(_.sink).foreach { s =>
              metadata = metadata.add("sink", uriSerializer.serialize(s.toEclipse))
            }

            attributes.flatMap(_.commstatus).foreach { cs =>
              metadata = metadata.add("commstatus", cs.value.toString)
            }

            attributes.flatMap(_.reqid).foreach { ri =>
              metadata =
                metadata.add(
                  "reqid",
                  eclipseUprotocol.uuid.serializer.LongUuidSerializer.instance.serialize(ri.toEclipse)
                )
            }

            effects.reply[BytesValue](innerEventByteValue, metadata)
          }

      case PublishUMessage(_, None, _) =>
        effects.error("Cannot send message to unspecified recipient")

      case _ =>
        effects.ignore
    }
  }

  private val log = BaseDispatcherAction.log
  private def ceProtoFormat = BaseDispatcherAction.ceProtoFormat
  private def uriSerializer = BaseDispatcherAction.uriSerializer
  private def uuidFactory = BaseDispatcherAction.uuidFactory
  private def empty = BaseDispatcherAction.emptyProto

  private def notImplementedUStatus(what: String) =
    eclipseUprotocol.v1.UStatus.newBuilder()
      .setCode(eclipseUprotocol.v1.UCode.UNIMPLEMENTED)
      .setMessage(s"$what not implemented yet")
      .build()

  private def responseMessage(responseBytes: ByteString, attributes: UAttributes): UMessage =
    UMessage(
      Some(UAttributes(
        id = Some(uuidFactory.create().toKalix),
        msgType = UMessageType.UMESSAGE_TYPE_RESPONSE,
        source = attributes.sink,
        sink = attributes.source,
        priority = attributes.priority,
        reqid = attributes.reqid,
        traceparent = attributes.traceparent
      )),
      payload =
        Some(UPayload(
          data = UPayload.Data.Value(responseBytes),
          length = Some(responseBytes.size),
          format = UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF
        ))
    )

  private def doStuff(stuff: => Future[Any]) =
    effects.asyncEffect[Empty](
      stuff.map(_ => effects.reply(empty))
        .recover {
          case ex => effects.error(s"uSubscription RPC failed: ${ex.getClass.getTypeName}")
        }
    )
}

object BaseDispatcherAction {
//  val ceProtoFormat = new ProtobufFormat
  val ceProtoFormat = new JsonFormat
  val log = LoggerFactory.getLogger("DispatcherAction")
  val uriSerializer = eclipseUprotocol.uri.serializer.LongUriSerializer.instance
  val uuidFactory = eclipseUprotocol.uuid.factory.UuidFactory.Factories.UPROTOCOL.factory
  val emptyProto = Empty()
}
