package com.lightbend.uprotocol

import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.google.protobuf.wrappers.BytesValue
import com.lightbend.uprotocol.BaseDispatcherAction.ceProtoFormat
import com.lightbend.uprotocol.cloudevent.factory.KalixUCloudEvent
import com.lightbend.uprotocol.model.Implicits.{CloudEventOps, EclipseUAttributesOps, KalixUMessageOps, KalixUUriOps}
import com.lightbend.uprotocol.model.cloudevent.Event
import com.lightbend.uprotocol.model.common.UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF
import com.lightbend.uprotocol.model.common.{UAuthority, UEntity, UMessage, UPayload, UResource, UUri}
import com.lightbend.uprotocol.model.subscription.SubscriberInfo
import io.cloudevents.protobuf.WormholeProtoSerializer
import kalix.scalasdk.testkit.MockRegistry
import org.eclipse.uprotocol.cloudevent.factory.UCloudEvent
import org.eclipse.uprotocol.transport.builder.UAttributesBuilder
import org.eclipse.uprotocol.v1.UPriority
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.slf4j.LoggerFactory
import com.lightbend.uprotocol.model.Implicits._

import scala.concurrent.Future


// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class DispatcherActionSpec
    extends AsyncWordSpec
    with Matchers
    with AsyncMockFactory {
  "DispatcherAction" must {


    "handle incoming Publish UMessage when there are NO subscribers" in {
      // Build Mock Subscription Manager
      val mockSubscriptionManager = stub[Topics]
      (mockSubscriptionManager.fetchSubscribers _)
        .when(*)
        .returns(Future.successful(FetchResponse(Seq.empty, None)))

      val mockRegistry = MockRegistry.withMock(mockSubscriptionManager)
      val service = DispatcherActionTestKit(new DispatcherAction(_), mockRegistry)

      // Build CloudEvent
      val kalixUMessage = buildPublishUMessage()
      val eclipseUMessage = kalixUMessage.toEclipse
      val cloudEvent = KalixUCloudEvent.fromMessage(eclipseUMessage)
      val cloudEventBytes = ceProtoFormat.serialize(cloudEvent)

      val result = service.vehicleIn(BytesValue(ByteString.copyFrom(cloudEventBytes)))
      // verify the reply
      result.asyncResult.map{ res =>
        res.isError shouldBe false
        res.reply shouldBe Empty()
      }
    }

    "handle incoming Publish UMessage when there are subscribers" in {
      // Build Mock Subscription Manager
      val outputId = 1
      val mockSubscriptionManager = stub[Topics]
      (mockSubscriptionManager.fetchSubscribers _)
        .when(*)
        .returns(Future.successful(FetchResponse(
          Seq(FetchItem(Some(SubscriberInfo(Some(buildUUri()))), outputId)),None)))

      // Build Mock Dispatcher
      val mockDispatcher = stub[Dispatcher]
      (mockDispatcher.vehicleOut _)
        .when(*)
        .returns(Future.successful(buildCloudEvent()))
      (mockDispatcher.cloudServiceOut1 _)
        .when(*)
        .returns(Future.successful(buildCloudEvent()))
      (mockDispatcher.cloudServiceOut2 _)
        .when(*)
        .returns(Future.successful(buildCloudEvent()))

      val mockRegistry = MockRegistry.withMock(mockSubscriptionManager).withMock(mockDispatcher)
      val service = DispatcherActionTestKit(new DispatcherAction(_), mockRegistry)

      // Build CloudEvent
      val kalixUMessage = buildPublishUMessage()
      val eclipseUMessage = kalixUMessage.toEclipse
      val cloudEvent = KalixUCloudEvent.fromMessage(eclipseUMessage)
      val cloudEventBytes = ceProtoFormat.serialize(cloudEvent)

      val result = service.vehicleIn(BytesValue(ByteString.copyFrom(cloudEventBytes)))
      // verify the reply
      result.asyncResult.map { res =>
        res.isError shouldBe false
        res.reply shouldBe Empty()
      }
    }

    "handle incoming Publish UMessages and distributing them to different outputs" in {
      // Build Mock Subscription Manager
      val outputId1 = 1
      val outputId2 = 2
      val mockSubscriptionManager = stub[Topics]
      (mockSubscriptionManager.fetchSubscribers _)
        .when(*)
        .returns(Future.successful(FetchResponse(
          Seq(FetchItem(Some(SubscriberInfo(Some(buildUUri()))), outputId1)),None)))

      // Build Mock Dispatcher
      val mockDispatcher = stub[Dispatcher]
      (mockDispatcher.cloudServiceOut1 _)
        .when(*)
        .returns(Future.successful(buildCloudEvent()))

      val mockRegistry = MockRegistry.withMock(mockSubscriptionManager).withMock(mockDispatcher)
      val service = DispatcherActionTestKit(new DispatcherAction(_), mockRegistry)

      // Build CloudEvent
      val kalixUMessage = buildPublishUMessage()
      val eclipseUMessage = kalixUMessage.toEclipse
      val cloudEvent = KalixUCloudEvent.fromMessage(eclipseUMessage)
      val cloudEventBytes = ceProtoFormat.serialize(cloudEvent)

      val result = service.vehicleIn(BytesValue(ByteString.copyFrom(cloudEventBytes)))
      // verify the reply
      result.asyncResult.map { res =>
        res.isError shouldBe false
        res.reply shouldBe Empty()
      }
    }

    "publish incoming UMessage to a topic" in {
      val service = DispatcherActionTestKit(new DispatcherAction(_))

      val publishUMessage = PublishUMessage(Some(buildPublishUMessage()), Some(buildUUri()))
      val result = service.vehicleOut(publishUMessage)
      val resultUMessage = toUMessage(result.reply);
      //TODO authority id is lost in mapping
      resultUMessage.toEclipse.getAttributes.getSource.getAuthority.getName shouldBe buildPublishUMessage().toEclipse.getAttributes.getSource.getAuthority.getName
    }

  }

  private def buildPublishUMessage(): UMessage = {
    val authority = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entity = Some(UEntity("body.access", None, Some(1), None))
    val resource = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuri = UUri(authority, entity, resource)
    val attributes = UAttributesBuilder.publish(uuri.toEclipse, UPriority.UPRIORITY_CS1).build()
    val payload = UPayload(UPayload.Data.Value(Empty().toByteString), None, UPAYLOAD_FORMAT_PROTOBUF)
    UMessage(Some(attributes.toKalix), Some(payload))
  }

  private def buildRequestUMessage(): UMessage = {
    val authority = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entity = Some(UEntity("body.access", None, Some(1), None))
    val resource = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuri = UUri(authority, entity, resource)
    val attributes = UAttributesBuilder.request(uuri.toEclipse, uuri.toEclipse, UPriority.UPRIORITY_CS1, 999).build()
    val payload = UPayload()
    UMessage(Some(attributes.toKalix), Some(payload))
  }

  private def buildResponseUMessage(requestId: org.eclipse.uprotocol.v1.UUID): UMessage = {
    val authority = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entity = Some(UEntity("body.access", None, Some(1), None))
    val resource = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuri = UUri(authority, entity, resource)
    val attributes = UAttributesBuilder.response(uuri.toEclipse, uuri.toEclipse, UPriority.UPRIORITY_CS1, requestId).build()
    val payload = UPayload()
    UMessage(Some(attributes.toKalix), Some(payload))
  }

  private def buildUUri(): UUri = {
    val authority = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entity = Some(UEntity("body.access", None, Some(1), None))
    val resource = Some(UResource("door", Some("front_left"), Some("Door"), None))
    UUri(authority, entity, resource)
  }

  private def buildCloudEvent(): BytesValue = {

//    val ce = WormholeProtoSerializer.toProto(UCloudEvent.fromMessage(buildPublishUMessage().toEclipse))
//    ce.toKalix
    val innerEvent = UCloudEvent.fromMessage(buildPublishUMessage().toEclipse)
    BytesValue.of(ByteString.copyFrom(ceProtoFormat.serialize(innerEvent)))
  }

  private def toUMessage(bytesValue: BytesValue): UMessage = {
    val outerCloudEvent = ceProtoFormat.deserialize(bytesValue.value.toByteArray)
    val eclipseUMessage = KalixUCloudEvent.toMessage(outerCloudEvent)
     eclipseUMessage.toKalix
  }
}
