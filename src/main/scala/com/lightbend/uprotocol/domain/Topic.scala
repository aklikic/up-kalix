package com.lightbend.uprotocol.domain

import com.lightbend.uprotocol
import com.lightbend.uprotocol.model.Implicits._
import org.eclipse.uprotocol.uri.serializer.LongUriSerializer
import com.google.protobuf.empty.Empty
import com.lightbend.uprotocol.FetchItem
import io.grpc.Status
import kalix.scalasdk.eventsourcedentity.{EventSourcedEntity, EventSourcedEntityContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.SortedSet
import scala.math.Ordering
import java.time.Instant

// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class Topic(context: EventSourcedEntityContext, clock: () => Instant) extends AbstractTopic {
  import EventSourcedEntity.Effect
  import Topic.log

  override def subscribe(
      state: TopicState,
      requestToSubscribe: uprotocol.RequestToSubscribe): Effect[uprotocol.SubscriptionResponse] = {
//    log.info("subscribe: state:{}; req:{}",state,requestToSubscribe)
    requestToSubscribe.subscriber
      .fold[Effect[uprotocol.SubscriptionResponse]](
        effects.error("Must specify a subscriber", Status.Code.INVALID_ARGUMENT)
      ) { subscriber =>
        subscriber.uri.map { u => LongUriSerializer.instance.serialize(u.toEclipse) }
          .fold[Effect[uprotocol.SubscriptionResponse]](
            effects.error("Subscriber must have a URI", Status.Code.INVALID_ARGUMENT)
          ) { uri =>
            state.subscribers.get(uri) match {
              case None =>
                // New subscription
                effects.emitEvent(
                  Subscribed(
                    subscriber = requestToSubscribe.subscriber,
                    attributes = requestToSubscribe.attributes,
                    asOf = Some(uprotocol.ProtoInstant.toProto(clock())),
                    outputId = requestToSubscribe.outputId
                  )
                ).thenReply { newState =>
                  val subscription = newState.subscribers(uri)
                  uprotocol.SubscriptionResponse(
                    status = subscription.status,
                    config = None,
                    topic = Some(topicAsString)
                  )
                }

              case Some(subscription) =>
                effects.reply(
                  uprotocol.SubscriptionResponse(
                    status = subscription.status,
                    config = None,
                    topic = Some(topicAsString)
                  ))
            }
          }
      }
  }

  override def unsubscribe(state: TopicState, requestToUnsubscribe: uprotocol.RequestToUnsubscribe): Effect[Empty] =
    requestToUnsubscribe.subscriber
      .flatMap { subscriber =>
        subscriber.uri
          .map { u => LongUriSerializer.instance.serialize(u.toEclipse) }
          .flatMap(state.subscribers.get)
          .map { subscription =>
            effects.emitEvent(Unsubscribed(requestToUnsubscribe.subscriber))
              .thenReply(_ => Empty.defaultInstance)
          }
      }
      .getOrElse(emptyReply)

  override def fetchSubscribers(
      state: TopicState,
      fetchRequest: uprotocol.FetchRequest): Effect[uprotocol.FetchResponse] = {
    if (state.subscribers.nonEmpty) {
      import Topic.SubscriptionStateOrdering

      val offset = fetchRequest.offset.getOrElse(Int.MinValue)
      
      val numBatches = (state.serializedSize >> 18).max(1)
      val numPerBatch = (state.subscribers.size / numBatches).max(1)

      val (selected, anotherBatch) =
        state.subscribers.iterator
          .map(_._2)
          .filter(_.count >= offset)
          .foldLeft(SortedSet.empty[SubscriptionState] -> false) { (acc, incoming) =>
            val (sorted, anyDeferred) = acc

            if (sorted.size < numPerBatch) sorted.incl(incoming) -> anyDeferred
            else if (sorted.head.count == incoming.count) {
              // belt and suspenders for if we happen to have multiple subscriptions with the same count
              // (e.g. bug)
              log.warn("Duplicate subscriber counts found")
              sorted.incl(incoming) -> anyDeferred
            } else {
              // don't include this one in the result, defer for next batch
              if (anyDeferred) acc    // don't allocate if we don't have to
              else sorted -> true
            }
          }

        val nextCursor = if (anotherBatch) Some(selected.last.count + 1) else None
//        val subscriberInfos = selected.iterator.flatMap(_.info).toSeq
//        effects.reply(uprotocol.FetchResponse(subscriberInfos, nextCursor))
        val items = selected.iterator.map(s => FetchItem(s.info,s.outputId)).toSeq
        effects.reply(uprotocol.FetchResponse(items, nextCursor))
    } else effects.reply(uprotocol.FetchResponse())
  }

  override def emptyState: TopicState = TopicState()

  override def subscribed(currentState: TopicState, subscribed: Subscribed): TopicState =
    subscribed.subscriber
      .flatMap { subscriberInfo => subscriberInfo.uri.map(subscriberInfo -> _) }
      .fold(currentState) { case (subscriberInfo, subscriberUri) =>
        val longUri = LongUriSerializer.instance.serialize(subscriberUri.toEclipse)

        currentState.subscribers.get(longUri) match {
          case None =>
            // New subscription
            import com.lightbend.uprotocol.model.subscription.SubscriptionStatus

            val count = currentState.cumulativeSubscriberCount + 1
            val newSubState =
              SubscriptionState(
                info = Some(subscriberInfo),
                attributes = subscribed.attributes,
                subscribedAt = subscribed.asOf,
                status = Some(SubscriptionStatus(state = SubscriptionStatus.State.SUBSCRIBED)),
                count = count,
                outputId = subscribed.outputId
              )

            currentState.copy(
              subscribers = currentState.subscribers.updated(longUri, newSubState),
              cumulativeSubscriberCount = count
            )

          case Some(_) =>
            // already subscribed, shouldn't happen...
            currentState
        }
      }

  override def unsubscribed(currentState: TopicState, unsubscribed: Unsubscribed): TopicState =
    unsubscribed.subscriber
      .flatMap(_.uri.map { u => LongUriSerializer.instance.serialize(u.toEclipse) })
      .filter(currentState.subscribers.contains)
      .fold(currentState) { subscriberUri =>
        currentState.copy(subscribers = currentState.subscribers.removed(subscriberUri))
      }

  override def registered(currentState: TopicState, registered: Registered): TopicState =
    registered.subscriber
      .flatMap { subscriberInfo => subscriberInfo.uri.map(subscriberInfo -> _) }
      .fold(currentState) { case (subscriberInfo, subscriberUri) =>
        val longUri = LongUriSerializer.instance.serialize(subscriberUri.toEclipse)

        currentState.notifications.get(longUri) match {
          case None =>
            // New notification subscription
            currentState.copy(notifications = currentState.notifications.updated(longUri, subscriberInfo))

          case Some(_) => currentState
        }
      }

  override def unregistered(currentState: TopicState, unregistered: Unregistered): TopicState =
    unregistered.subscriber
      .flatMap(_.uri.map { u => LongUriSerializer.instance.serialize(u.toEclipse) })
      .filter(currentState.notifications.contains)
      .fold(currentState) { subscriberUri =>
        currentState.copy(notifications = currentState.notifications.removed(subscriberUri))
      }

  override def subscriptionsReset(currentState: TopicState, subscriptionsReset: SubscriptionsReset): TopicState =
    subscriptionsReset.before
      .map(uprotocol.ProtoInstant.fromProto)
      .fold(currentState.copy(subscribers = Map.empty)) { before =>
        currentState.copy(
          subscribers =
            currentState.subscribers.filter {
              case (_, info) =>
                info.subscribedAt.exists { subscribedAt =>
                  uprotocol.ProtoInstant.fromProto(subscribedAt).isAfter(before)
                }
            }
        )
      }

  private val topicAsString = LongUriSerializer.instance.deserialize(context.entityId).toKalix
  private val emptyReply = effects.reply(Empty.defaultInstance)
}

object Topic {
  val log = LoggerFactory.getLogger(getClass.getName.replaceAll("$", ""))
  implicit val SubscriptionStateOrdering: Ordering[SubscriptionState] = Ordering.by(_.count)
}
