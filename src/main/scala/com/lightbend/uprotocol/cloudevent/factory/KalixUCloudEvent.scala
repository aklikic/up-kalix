/*
 * Copyright (C) Lightbend, Inc.
 */
package com.lightbend.uprotocol.cloudevent.factory

import com.google.protobuf.{Any => ProtoAny}
import com.google.protobuf.{InvalidProtocolBufferException, Message}
import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.core.format.EventFormat
import io.cloudevents.core.provider.EventFormatProvider
import io.cloudevents.protobuf.ProtobufFormat
import io.cloudevents.v1.proto.{CloudEvent => CloudEventAsProto}
import org.eclipse.uprotocol.UprotocolOptions
import org.eclipse.uprotocol.cloudevent.factory._
import org.eclipse.uprotocol.uri.serializer.LongUriSerializer
import org.eclipse.uprotocol.uuid.serializer.LongUuidSerializer
import org.eclipse.uprotocol.v1.{UAttributes, UCode, UMessage}

import scala.jdk.OptionConverters._
import scala.reflect.ClassTag
import java.lang.{Long => JLong}
import java.net.URI
import java.util.{Objects, Optional}

object KalixUCloudEvent extends UCloudEvent {
  // Java API
  def getCommunicationStatus(ce: CloudEvent): UCode = communicationStatus(ce)
  def getCreationTimestamp(ce: CloudEvent): Optional[JLong] = UCloudEvent.getCreationTimestamp(ce)
  def getHash(ce: CloudEvent): Optional[String] = hash(ce).toJava
  def getKalixUCEVersion(ce: CloudEvent): Optional[Integer] = ttl(ce).map(Int.box).toJava
  def getPriority(ce: CloudEvent): Optional[String] = priority(ce).toJava
  def getRequestId(ce: CloudEvent): Optional[String] = requestId(ce).toJava
  def getSink(ce: CloudEvent): Optional[String] = sink(ce).toJava
  def getSource(ce: CloudEvent): String = source(ce)
  def getToken(ce: CloudEvent): Optional[String] = token(ce).toJava
  def getTraceparent(ce: CloudEvent): Optional[String] = traceparent(ce).toJava
  def getTtl(ce: CloudEvent): Optional[Integer] = ttl(ce).map(Int.box).toJava
  def getPayload(ce: CloudEvent): ProtoAny = payload(ce)
  def unpack[T <: Message](ce: CloudEvent, clazz: Class[T]): Optional[T] = unpack(ce)(ClassTag(clazz)).toJava

  // Both Java and Scala API
  def addCommunicationStatus(ce: CloudEvent, communicationStatus: Integer): CloudEvent =
    communicationStatus match {
      case null => ce
      case status =>
        mapCloudEvent(ce) { cloudEvent =>
          val isWrapped = cloudEvent eq ce

          buildFrom(ce) { builder =>
            // "outer" cloud event
            builder.withExtension("commstatus", status)

            if (isWrapped) {
              val newInnerCE = buildFrom(cloudEvent)(_.withExtension("commstatus", status))

              builder.withData(eventFormat.serialize(newInnerCE))
            } else builder
          }
        }
        .getOrElse { throw new RuntimeException(s"Could not addCommunicationStatus [$status] to [${toString(ce)}]") }
    }

  def fromMessage(message: UMessage): CloudEvent = {
    val innerEvent = UCloudEvent.fromMessage(message)
    val attributes = Objects.requireNonNullElse(message.getAttributes, UAttributes.getDefaultInstance)

    val builder =
      CloudEventBuilder.v1
        .withId(java.util.UUID.randomUUID().toString)
        .withType(message.getAttributes.getType.getValueDescriptor.getOptions.getExtension(UprotocolOptions.ceName))
        .withSource(URI.create(LongUriSerializer.instance.serialize(attributes.getSource)))
        .withData(eventFormat.serialize(innerEvent))
        .withDataContentType("application/octet-stream")

    if (attributes.hasTtl) { builder.withExtension("ttl", attributes.getTtl) }
    if (attributes.hasToken) { builder.withExtension("token", attributes.getToken) }
    if (attributes.hasPermissionLevel) { builder.withExtension("plevel", attributes.getPermissionLevel) }
    if (attributes.hasTraceparent) { builder.withExtension("traceparent", attributes.getTraceparent) }

    if (attributes.getPriorityValue > 0) {
      builder.withExtension("priority", UCloudEvent.getCePriority(attributes.getPriority))
    }

    if (attributes.hasSink) {
      builder.withExtension("sink", URI.create(LongUriSerializer.instance.serialize(attributes.getSink)))
    }

    if (attributes.hasCommstatus) {
      builder.withExtension("commstatus", attributes.getCommstatus.getNumber)
    }

    if (attributes.hasReqid) {
      builder.withExtension("reqid", LongUuidSerializer.instance.serialize(attributes.getReqid))
    }

    builder.build
  }

  def hasCommunicationStatusProblem(ce: CloudEvent): Boolean = hasCommunicationStatusProblem(new Extensions(ce))

  def isCloudEventId(ce: CloudEvent): Boolean =
    mapCloudEvent(ce)(UCloudEvent.isCloudEventId)
      .getOrElse { throw new AssertionError("unreachable was reached?") }

  def isExpiredByCloudEventCreationDate(ce: CloudEvent): Boolean = UCloudEvent.isExpiredByCloudEventCreationDate(ce)
  def isExpired(ce: CloudEvent): Boolean = UCloudEvent.isExpired(ce)

  def toMessage(ce: CloudEvent): UMessage = {
    require(ce ne null, "UMessage extraction requires non-null CloudEvent")

    mapCloudEvent(ce)(UCloudEvent.toMessage)
      .getOrElse { throw new RuntimeException(s"Could not extract UMessage from Kalix CloudEvent") }
  }

  def toString(ce: CloudEvent): String =
    mapCloudEvent(ce)(UCloudEvent.toString).getOrElse { throw new AssertionError("unreachable was reached?") }

  // Scala API (matching Java API)
  def communicationStatus(ce: CloudEvent): UCode = communicationStatus(new Extensions(ce))
  def creationTimestamp(ce: CloudEvent): Option[Long] = getCreationTimestamp(ce).toScala.map(Long.unbox)
  def hash(ce: CloudEvent): Option[String] = hash(new Extensions(ce))
  def kalixUCEVersion(ce: CloudEvent): Option[Int] = kalixUCEVersion(new Extensions(ce))

  def payload(ce: CloudEvent): ProtoAny = {
    // This is just an optimized version of `mapCloudEvent(UCloudEvent.getPayload)`: in the case where
    // this gets called on a "regular" CloudEvent, it will only call `UCloudEvent.getPayload` once
    val outerPayloadAny = UCloudEvent.getPayload(ce)

    unpackAny[CloudEventAsProto](outerPayloadAny)
      .fold(outerPayloadAny) { proto =>
        UCloudEvent.getPayload(eventFormat.deserialize(proto.toByteArray))
      }
  }

  def priority(ce: CloudEvent): Option[String] = priority(new Extensions(ce))
  def requestId(ce: CloudEvent): Option[String] = requestId(new Extensions(ce))
  def sink(ce: CloudEvent): Option[String] = sink(new Extensions(ce))
  def source(ce: CloudEvent): String = ce.getSource.toString
  def token(ce: CloudEvent): Option[String] = token(new Extensions(ce))
  def traceparent(ce: CloudEvent): Option[String] = traceparent(new Extensions(ce))
  def ttl(ce: CloudEvent): Option[Int] = ttl(new Extensions(ce))
  def unpack[T <: Message : ClassTag](ce: CloudEvent): Option[T] = unpackAny(payload(ce))

  // Scala API, can reuse "Extensions" wrapper
  def communicationStatus(e: Extensions): UCode =
    e.extractInt("commstatus")
      .flatMap(cs => Option(UCode.forNumber(cs)))
      .getOrElse(UCode.OK)

  def hash(e: Extensions): Option[String] = e.extractString("hash")

  def hasCommunicationStatusProblem(e: Extensions): Boolean =
    communicationStatus(e) != UCode.OK

  def kalixUCEVersion(e: Extensions): Option[Int] = e.extractInt("kalix-uce-version")
  def priority(e: Extensions): Option[String] = e.extractString("priority")
  def requestId(e: Extensions): Option[String] = e.extractString("reqid")
  def sink(e: Extensions): Option[String] = e.extractString("sink")
  def source(e: Extensions): String = source(e.ce)

  def unpack[T <: Message : Class](e: Extensions): Option[T] =
    try {
      Option(payload(e.ce).unpack(implicitly[Class[T]]))
    } catch {
      case e: InvalidProtocolBufferException => None
    }

  def token(e: Extensions): Option[String] = e.extractString("token")
  def traceparent(e: Extensions): Option[String] = e.extractString("traceparent")
  def ttl(e: Extensions): Option[Int] = e.extractInt("ttl")

  class Extensions(val ce: CloudEvent) {
    def extractString(name: String): Option[String] =
      if (names.contains(name)) Option(ce.getExtension(name)).map(String.valueOf)
      else                      None

    def extractInt(name: String): Option[Int] =
      if (names.contains(name)) {
        Option(ce.getExtension(name))
          .map {
            _ match {
              case i: Integer => i
              case x: Any => Integer.valueOf(x.toString)
            }
          }
      } else None

    // lighter-weight than lazy val... compute at-least-never (rather than hard-at-most-once)
    def names: java.util.Set[String] = {
      if (_names eq null) {
        _names = ce.getExtensionNames
      }

      _names
    }

    private var _names: java.util.Set[String] = null
  }

  private def mapCloudEvent[T](ce: CloudEvent)(f: CloudEvent => T): Option[T] = {
    unpackAny[CloudEventAsProto](UCloudEvent.getPayload(ce))
      .fold(Option(f(ce))) { proto =>
        Option(f(eventFormat.deserialize(proto.toByteArray)))
      }
  }

  private def unpackAny[T <: Message : ClassTag](any: ProtoAny): Option[T] =
    try {
      Option(any.unpack(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]))
    } catch {
      case e: InvalidProtocolBufferException => None
    }

  private def eventFormat: EventFormat = {
    if (_eventFormat eq null) {
      _eventFormat = EventFormatProvider.getInstance().resolveFormat(ProtobufFormat.PROTO_CONTENT_TYPE)
    }

    _eventFormat
  }

  private var _eventFormat: EventFormat = null

  private def buildFrom(ce: CloudEvent)(withBuilder: CloudEventBuilder => CloudEventBuilder): CloudEvent =
    withBuilder(CloudEventBuilder.v1(ce))
      .build()
}
