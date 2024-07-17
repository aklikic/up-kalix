package com.lightbend.uprotocol.model

import com.google.protobuf.any.{ Any => ProtoAny }
import com.google.protobuf.timestamp.Timestamp
import io.cloudevents.v1.proto.{ CloudEvent => CloudEventProto }
import org.eclipse.{ uprotocol => eclipseUprotocol }

import java.util.{ List => JList }

import scala.jdk.CollectionConverters.{ IteratorHasAsScala, MapHasAsScala }
import com.lightbend.uprotocol.model.cloudevent.Event.CloudEventAttributeValue

/** Extensions to model protobufs and Eclipse uProtocol protobufs to allow for
 * conversion without a serialization roundtrip
 */
object Implicits {
  implicit class KalixUCodeOps(val kalix: common.UCode) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UCode = eclipseUprotocol.v1.UCode.forNumber(kalix.value)
  }

  implicit class EclipseUCodeOps(val eclipse: eclipseUprotocol.v1.UCode) extends AnyVal {
    def toKalix: common.UCode = common.UCode.fromValue(eclipse.getNumber)
  }

  implicit class KalixUResourceOps(val kalix: common.UResource) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UResource = {
      val builder = eclipseUprotocol.v1.UResource.newBuilder().setName(kalix.name)

      builder.setName(kalix.name)
      kalix.instance.foreach { instance => builder.setInstance(instance) }
      kalix.message.foreach { message => builder.setMessage(message) }
      kalix.id.foreach { id => builder.setId(id) }

      builder.build
    }
  }

  implicit class EclipseUResourceOps(val eclipse: eclipseUprotocol.v1.UResource) extends AnyVal {
    def toKalix: common.UResource = {
      val stringToOption = protoStringToOption(eclipse) _

      common.UResource(
        name = eclipse.getName,
        instance = stringToOption(_.hasInstance, _.getInstance),
        message = stringToOption(_.hasMessage, _.getMessage),
        id = protoInt32ToOption(eclipse)(_.hasId, _.getId)
      )
    }
  }

  implicit class KalixUEntityOps(val kalix: common.UEntity) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UEntity = {
      val builder = eclipseUprotocol.v1.UEntity.newBuilder().setName(kalix.name)
      
      kalix.id.foreach { id => builder.setId(id) }
      kalix.versionMajor.foreach { v => builder.setVersionMajor(v) }
      kalix.versionMinor.foreach { v => builder.setVersionMinor(v) }

      builder.build
    }
  }

  implicit class EclipseUEntityOps(val eclipse: eclipseUprotocol.v1.UEntity) extends AnyVal {
    def toKalix: common.UEntity = {
      val intToOption = protoInt32ToOption(eclipse) _

      common.UEntity(
        name = eclipse.getName,
        id = intToOption(_.hasId, _.getId),
        versionMajor = intToOption(_.hasVersionMajor, _.getVersionMajor),
        versionMinor = intToOption(_.hasVersionMinor, _.getVersionMinor)
      )
    }
  }

  implicit class KalixUAuthorityOps(val kalix: common.UAuthority) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UAuthority = {
      val builder = eclipseUprotocol.v1.UAuthority.newBuilder()

      kalix.name.foreach { name => builder.setName(name) }
      kalix.number.ip.foreach(builder.setIp)
      kalix.number.id.foreach(builder.setId)

      builder.build
    }
  }

  implicit class EclipseUAuthorityOps(val eclipse: eclipseUprotocol.v1.UAuthority) extends AnyVal {
    def toKalix: common.UAuthority = {
      val byteStringToOption = protoByteStringToOption(eclipse) _

      val number =
        byteStringToOption(_.hasIp, _.getIp).map(common.UAuthority.Number.Ip)
          .orElse {
            byteStringToOption(_.hasId, _.getId).map(common.UAuthority.Number.Id)
          }
          .getOrElse(common.UAuthority.Number.Empty)

      common.UAuthority(
        name = protoStringToOption(eclipse)(_.hasName, _.getName),
        number = number
      )
    }
  }

  implicit class KalixUUriOps(val kalix: common.UUri) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UUri = {
      val builder = eclipseUprotocol.v1.UUri.newBuilder()

      kalix.authority.foreach { a => builder.setAuthority(a.toEclipse) }
      kalix.entity.foreach { e => builder.setEntity(e.toEclipse) }
      kalix.resource.foreach { r => builder.setResource(r.toEclipse) }

      builder.build
    }
  }

  implicit class EclipseUUriOps(val eclipse: eclipseUprotocol.v1.UUri) extends AnyVal {
    def toKalix: common.UUri =
      common.UUri(
        authority = if (eclipse.hasAuthority) Some(eclipse.getAuthority.toKalix) else None,
        entity = if (eclipse.hasEntity) Some(eclipse.getEntity.toKalix) else None,
        resource = if (eclipse.hasResource) Some(eclipse.getResource.toKalix) else None
      )
  }

  implicit class KalixSubscribeAttributesOps(val kalix: subscription.SubscribeAttributes) extends AnyVal {
    import com.google.protobuf.any.Any

    def toEclipse: eclipseUprotocol.core.usubscription.v3.SubscribeAttributes = {
      val builder = eclipseUprotocol.core.usubscription.v3.SubscribeAttributes.newBuilder

      kalix.expire.foreach { e => builder.setExpire(Timestamp.toJavaProto(e)) }

      kalix.details.iterator.map(Any.toJavaProto).foreach(builder.addDetails)
      kalix.samplePeriodMs.foreach(builder.setSamplePeriodMs)

      builder.build
    }
  }

  implicit class EclipseSubscribeAttributesOps(
      val eclipse: eclipseUprotocol.core.usubscription.v3.SubscribeAttributes) extends AnyVal {
    def toKalix: subscription.SubscribeAttributes =
      subscription.SubscribeAttributes(
        details = eclipse.getDetailsList.toKalix,
        expire = if (eclipse.hasExpire) Some(Timestamp.fromJavaProto(eclipse.getExpire)) else None,
        samplePeriodMs = if (eclipse.hasSamplePeriodMs) Some(eclipse.getSamplePeriodMs) else None
      )
  }

  implicit class KalixSubscriberInfo(val kalix: subscription.SubscriberInfo) extends AnyVal {
    import com.google.protobuf.any.Any

    def toEclipse: eclipseUprotocol.core.usubscription.v3.SubscriberInfo = {
      val builder = eclipseUprotocol.core.usubscription.v3.SubscriberInfo.newBuilder

      kalix.uri.foreach(u => builder.setUri(u.toEclipse))
      kalix.details.iterator.map(Any.toJavaProto).foreach(builder.addDetails)

      builder.build
    }
  }

  implicit class EclipseSubscriberInfo(
      val eclipse: eclipseUprotocol.core.usubscription.v3.SubscriberInfo) extends AnyVal {
    def toKalix: subscription.SubscriberInfo =
      subscription.SubscriberInfo(
        uri = if (eclipse.hasUri) Some(eclipse.getUri.toKalix) else None,
        details = eclipse.getDetailsList.toKalix
      )
  }

  implicit class KalixSubscriptionStatusOps(val kalix: subscription.SubscriptionStatus) extends AnyVal {
    def toEclipse: eclipseUprotocol.core.usubscription.v3.SubscriptionStatus = {
      val builder =
        eclipseUprotocol.core.usubscription.v3.SubscriptionStatus.newBuilder

      builder.setState(
        eclipseUprotocol.core.usubscription.v3.SubscriptionStatus.State.forNumber(kalix.state.value)
      )
      builder.setCode(kalix.code.toEclipse)
      builder.setMessage(kalix.message)

      builder.build
    }
  }

  implicit class EclipseSubscriptionStatusOps(
      val eclipse: eclipseUprotocol.core.usubscription.v3.SubscriptionStatus) extends AnyVal {
    def toKalix: subscription.SubscriptionStatus =
      subscription.SubscriptionStatus(
        state = subscription.SubscriptionStatus.State.fromValue(eclipse.getStateValue),
        code = eclipse.getCode.toKalix,
        message = eclipse.getMessage
      )
  }

  implicit class KalixEventDeliveryConfigOps(val kalix: subscription.EventDeliveryConfig) extends AnyVal {
    import com.google.protobuf.any.Any

    def toEclipse: eclipseUprotocol.core.usubscription.v3.EventDeliveryConfig = {
      val builder = eclipseUprotocol.core.usubscription.v3.EventDeliveryConfig.newBuilder

      builder.setId(kalix.id).setType(kalix.`type`)
      kalix.attributes.foreach {
        case (k, v) => builder.putAttributes(k, Any.toJavaProto(v))
      }

      builder.build
    }
  }

  implicit class EclipseEventDeliveryConfigOps(
      val eclipse: eclipseUprotocol.core.usubscription.v3.EventDeliveryConfig) extends AnyVal {
    import com.google.protobuf.any.Any

    def toKalix: subscription.EventDeliveryConfig =
      subscription.EventDeliveryConfig(
        id = eclipse.getId,
        `type` = eclipse.getType,
        attributes =
          eclipse.getAttributesMap.asScala.view.mapValues(Any.fromJavaProto).toMap
      )
  }

  implicit class KalixResetRequestOps(val kalix: subscription.ResetRequest) extends AnyVal {
    def toEclipse: eclipseUprotocol.core.usubscription.v3.ResetRequest = {
      val builder = eclipseUprotocol.core.usubscription.v3.ResetRequest.newBuilder

      kalix.before.foreach { b => builder.setBefore(Timestamp.toJavaProto(b)) }
      kalix.reason.foreach { r =>
        val reasonBuilder = eclipseUprotocol.core.usubscription.v3.ResetRequest.Reason.newBuilder

        reasonBuilder
          .setCode(
            eclipseUprotocol.core.usubscription.v3.ResetRequest.Reason.Code.forNumber(r.code.value)
          )

        r.message.foreach(reasonBuilder.setMessage)

        builder.setReason(reasonBuilder.build)
      }

      builder.build
    }
  }

  implicit class EclipseResetRequestOps(
      val eclipse: eclipseUprotocol.core.usubscription.v3.ResetRequest) extends AnyVal {
    def toKalix: subscription.ResetRequest =
      subscription.ResetRequest(
        before = if (eclipse.hasBefore) Some(Timestamp.fromJavaProto(eclipse.getBefore)) else None,
        reason =
          if (eclipse.hasReason) {
            Some(
              subscription.ResetRequest.Reason(
                code = subscription.ResetRequest.Reason.Code.fromValue(eclipse.getReason.getCodeValue),
                message = if (eclipse.getReason.hasMessage) Some(eclipse.getReason.getMessage) else None
              )
            )
          } else None
      )
  }

  implicit class KalixUUIDOps(val kalix: common.UUID) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UUID =
      eclipseUprotocol.v1.UUID.newBuilder
        .setLsb(kalix.lsb)
        .setMsb(kalix.msb)
        .build
  }

  implicit class EclipseUUIDOps(val eclipse: eclipseUprotocol.v1.UUID) extends AnyVal {
    def toKalix: common.UUID = common.UUID(msb = eclipse.getMsb, lsb = eclipse.getLsb)
  }

  implicit class KalixUMessageTypeOps(val kalix: common.UMessageType) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UMessageType = eclipseUprotocol.v1.UMessageType.forNumber(kalix.value)
  }

  implicit class EclipseUMessageTypeOps(val eclipse: eclipseUprotocol.v1.UMessageType) extends AnyVal {
    def toKalix: common.UMessageType = common.UMessageType.fromValue(eclipse.getNumber)
  }

  implicit class KalixUPriorityOps(val kalix: common.UPriority) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UPriority = eclipseUprotocol.v1.UPriority.forNumber(kalix.value)
  }

  implicit class EclipseUPriorityOps(val eclipse: eclipseUprotocol.v1.UPriority) extends AnyVal {
    def toKalix: common.UPriority = common.UPriority.fromValue(eclipse.getNumber)
  }

  implicit class KalixUPayloadFormatOps(val kalix: common.UPayloadFormat) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UPayloadFormat = eclipseUprotocol.v1.UPayloadFormat.forNumber(kalix.value)
  }

  implicit class EclipseUPayloadFormatOps(val eclipse: eclipseUprotocol.v1.UPayloadFormat) extends AnyVal {
    def toKalix: common.UPayloadFormat = common.UPayloadFormat.fromValue(eclipse.getNumber)
  }

  implicit class KalixUAttributesOps(val kalix: common.UAttributes) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UAttributes = {
      val builder = eclipseUprotocol.v1.UAttributes.newBuilder

      builder.setType(kalix.msgType.toEclipse)
      builder.setPriority(kalix.priority.toEclipse)
      //builder.setPayloadFormat(kalix.payloadFormat.toEclipse)

      kalix.id.foreach { i => builder.setId(i.toEclipse) }
      kalix.source.foreach { s => builder.setSource(s.toEclipse) }
      kalix.sink.foreach { s => builder.setSink(s.toEclipse) }
      kalix.ttl.foreach(builder.setTtl)
      kalix.permissionLevel.foreach(builder.setPermissionLevel)
      kalix.commstatus.foreach { c => builder.setCommstatus(c.toEclipse) }
      kalix.reqid.foreach { r => builder.setReqid(r.toEclipse) }
      kalix.token.foreach(builder.setToken)
      kalix.traceparent.foreach(builder.setTraceparent)

      builder.build
    }
  }

  implicit class EclipseUAttributesOps(val eclipse: eclipseUprotocol.v1.UAttributes) extends AnyVal {
    def toKalix: common.UAttributes =
      common.UAttributes(
        id = if (eclipse.hasId) Some(eclipse.getId.toKalix) else None,
        msgType = eclipse.getType.toKalix,
        source = if (eclipse.hasSource) Some(eclipse.getSource.toKalix) else None,
        sink = if (eclipse.hasSink) Some(eclipse.getSink.toKalix) else None,
        priority = eclipse.getPriority.toKalix,
        ttl = protoInt32ToOption(eclipse)(_.hasTtl, _.getTtl),
        permissionLevel = protoInt32ToOption(eclipse)(_.hasTtl, _.getTtl),
        commstatus = if (eclipse.hasCommstatus) Some(eclipse.getCommstatus.toKalix) else None,
        reqid = if (eclipse.hasReqid) Some(eclipse.getReqid.toKalix) else None,
        token = protoStringToOption(eclipse)(_.hasToken, _.getToken),
        traceparent = protoStringToOption(eclipse)(_.hasToken, _.getToken),
        // payloadFormat =
      )
  }

  implicit class KalixUPayloadOps(val kalix: common.UPayload) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UPayload = {
      val builder = eclipseUprotocol.v1.UPayload.newBuilder

      builder.setFormat(kalix.format.toEclipse)

      kalix.data.reference.foreach(builder.setReference)
      kalix.data._value.foreach(builder.setValue)
      kalix.length.foreach(builder.setLength)

      builder.build
    }
  }

  implicit class EclipseUPayloadOps(val eclipse: eclipseUprotocol.v1.UPayload) extends AnyVal {
    def toKalix: common.UPayload =
      common.UPayload(
        format = eclipse.getFormat.toKalix,
        length = protoInt32ToOption(eclipse)(_.hasLength, _.getLength),
        data =
          if (eclipse.hasReference) common.UPayload.Data.Reference(eclipse.getReference)
          else if (eclipse.hasValue) common.UPayload.Data.Value(eclipse.getValue)
          else common.UPayload.Data.Empty,
      )
  }

  implicit class KalixUMessageOps(val kalix: common.UMessage) extends AnyVal {
    def toEclipse: eclipseUprotocol.v1.UMessage = {
      val builder = eclipseUprotocol.v1.UMessage.newBuilder

      kalix.attributes.foreach { a => builder.setAttributes(a.toEclipse) }
      kalix.payload.foreach { p => builder.setPayload(p.toEclipse) }

      builder.build
    }
  }

  implicit class EclipseUMessageOps(val eclipse: eclipseUprotocol.v1.UMessage) extends AnyVal {
    def toKalix: common.UMessage =
      common.UMessage(
        attributes = if (eclipse.hasAttributes) Some(eclipse.getAttributes.toKalix) else None,
        payload = if (eclipse.hasPayload) Some(eclipse.getPayload.toKalix) else None
      )
  }

  implicit class JListProtoAnyOps(val list: JList[com.google.protobuf.Any]) extends AnyVal {
    import com.google.protobuf.any.Any

    def toKalix: Seq[Any] = list.iterator.asScala.map(Any.fromJavaProto).toSeq
  }

  implicit class KalixCloudEventOps(val kalix: cloudevent.Event) extends AnyVal {
    def toCE: CloudEventProto = {
      val builder = CloudEventProto.newBuilder

      builder.setId(kalix.id)
      builder.setSource(kalix.source)
      builder.setSpecVersion(kalix.specVersion)
      builder.setType(kalix.`type`)

      kalix.data.binaryData.foreach(builder.setBinaryData)
      kalix.data.textData.foreach(builder.setTextData)
      kalix.data.protoData.foreach { p =>
        builder.setProtoData(ProtoAny.toJavaProto(p))
      }

      kalix.attributes.foreach {
        case (key, attributeValue) =>
          val attrBuilder = CloudEventProto.CloudEventAttributeValue.newBuilder

          attributeValue.attr.ceBoolean.foreach(attrBuilder.setCeBoolean)
          attributeValue.attr.ceBytes.foreach(attrBuilder.setCeBytes)
          attributeValue.attr.ceInteger.foreach(attrBuilder.setCeInteger)
          attributeValue.attr.ceString.foreach(attrBuilder.setCeString)
          attributeValue.attr.ceTimestamp.foreach { t => attrBuilder.setCeTimestamp(Timestamp.toJavaProto(t)) }
          attributeValue.attr.ceUri.foreach(attrBuilder.setCeUri)
          attributeValue.attr.ceUriRef.foreach(attrBuilder.setCeUriRef)

          builder.putAttributes(key, attrBuilder.build)
      }
      
      builder.build
    }
  }

  implicit class CloudEventOps(val ce: CloudEventProto) extends AnyVal {
    def toKalix: cloudevent.Event =
      cloudevent.Event(
        id = ce.getId,
        source = ce.getSource,
        specVersion = ce.getSpecVersion,
        `type` = ce.getType,
        data =
          if (ce.hasBinaryData) cloudevent.Event.Data.BinaryData(ce.getBinaryData)
          else if (ce.hasTextData) cloudevent.Event.Data.TextData(ce.getTextData)
          else if (ce.hasProtoData) cloudevent.Event.Data.ProtoData(ProtoAny.fromJavaProto(ce.getProtoData))
          else cloudevent.Event.Data.Empty,
        attributes =
          ce.getAttributesMap.asScala.view
            .mapValues { attr =>
              val outAttr =
                if (attr.hasCeBoolean) cloudevent.Event.CloudEventAttributeValue.Attr.CeBoolean(attr.getCeBoolean)
                else if (attr.hasCeBytes) cloudevent.Event.CloudEventAttributeValue.Attr.CeBytes(attr.getCeBytes)
                else if (attr.hasCeInteger) cloudevent.Event.CloudEventAttributeValue.Attr.CeInteger(attr.getCeInteger)
                else if (attr.hasCeString) cloudevent.Event.CloudEventAttributeValue.Attr.CeString(attr.getCeString)
                else if (attr.hasCeUri) cloudevent.Event.CloudEventAttributeValue.Attr.CeUri(attr.getCeUri)
                else if (attr.hasCeUriRef) cloudevent.Event.CloudEventAttributeValue.Attr.CeUriRef(attr.getCeUriRef)
                else if (attr.hasCeTimestamp) {
                  cloudevent.Event.CloudEventAttributeValue.Attr.CeTimestamp(Timestamp.fromJavaProto(attr.getCeTimestamp))
                } else cloudevent.Event.CloudEventAttributeValue.Attr.Empty

              CloudEventAttributeValue(outAttr)
            }
            .toMap
      )
  }

  def protoStringToOption[T <: com.google.protobuf.GeneratedMessageV3](obj: T)(
      hasField: T => Boolean,
      getter: T => String): Option[String] =
    if (hasField(obj)) Some(getter(obj)) else None

  def protoInt32ToOption[T <: com.google.protobuf.GeneratedMessageV3](obj: T)(
      hasField: T => Boolean,
      getter: T => Int): Option[Int] =
    if (hasField(obj)) Some(getter(obj)) else None

  def protoByteStringToOption[T <: com.google.protobuf.GeneratedMessageV3](obj: T)(
      hasField: T => Boolean,
      getter: T => com.google.protobuf.ByteString): Option[com.google.protobuf.ByteString] =
    if (hasField(obj)) Some(getter(obj)) else None
}
