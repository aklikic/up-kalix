package com.lightbend.uprotocol
import com.google.protobuf.ByteString
import com.google.rpc.{Code, Status}
import com.lightbend.uprotocol.cloudevent.factory.KalixUCloudEvent
import com.lightbend.uprotocol.model.Implicits._
import kalix.scalasdk.Metadata
import kalix.scalasdk.testkit.KalixTestKit
import org.eclipse.uprotocol.cloudevent.factory.UCloudEvent
import org.eclipse.uprotocol.v1._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, stats}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets


class IntegrationSpec extends AnyWordSpec
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with ScalaFutures{

  implicit private val patience: PatienceConfig =
    PatienceConfig(Span(5, Seconds), Span(500, Millis))


  private val testKit = KalixTestKit(
    Main.createKalix(),
    KalixTestKit.DefaultSettings
      .withTopicIncomingMessages("vehicle-in")
      .withTopicIncomingMessages("cloudservice-in")
      .withTopicOutgoingMessages("vehicle-out")
      .withTopicOutgoingMessages("cloudservice-out-1")
      .withTopicOutgoingMessages("cloudservice-out-2")
  ).start()

  private val vehicleInTopic = testKit.getTopicIncomingMessages("vehicle-in")
  private val cloudServiceInTopic = testKit.getTopicIncomingMessages("cloudservice-in")
  private val vehicleOutTopic = testKit.getTopicOutgoingMessages("vehicle-out")
  private val cloudServiceOut1Topic = testKit.getTopicOutgoingMessages("cloudservice-out-1")
  private val cloudServiceOut2Topic = testKit.getTopicOutgoingMessages("cloudservice-out-2")

  private val uEntityMetadatasClient = testKit.getGrpcClient(classOf[UEntityMetadatas])
  private val topicsClient = testKit.getGrpcClient(classOf[Topics])

  "service" must {

    "subscribe to a topic and handle routing of publish message" in {
      val outputId1 = 1
      val entityName = "myApp1"

      val outputId2 = 2
      val entityName2 = "myApp2"

      val vin = "111111"

      val subscribeToUri = buildDoorSubscribeToUri(vin)

      //provision cloud service
      uEntityMetadatasClient.addUEntityMetadata(AddUEntityMetadataRequest(entityName,outputId1)).futureValue
      //provision cloud service
      uEntityMetadatasClient.addUEntityMetadata(AddUEntityMetadataRequest(entityName2,outputId2)).futureValue

      //subscribe entity 1
      val subscriberUri = buildSubscriberUri(entityName)
      val subscribeRequestUMessage = UMessageHelper.createSubscriptionRpcUMessageV3(subscriberUri,subscribeToUri)
      //log.info("subscriberUri: {}, subscribeRequestUMessage:{}",subscriberUri,subscribeRequestUMessage)
      cloudServiceInTopic.publish(uMessageToByteString(subscribeRequestUMessage)/*,Metadata.empty.set("subject",entityName)*/)
      val subscribeResponse = cloudServiceOut1Topic.expectOneRaw()
      val subscribeResponseUMessage = extractUMessage(subscribeResponse.payload)
      subscribeResponseUMessage.toKalix.attributes.get.sink.get.entity.get.name shouldBe entityName

      //subscribe entity 1
      val subscribeRequestUMessage2 = UMessageHelper.createSubscriptionRpcUMessageV3(buildSubscriberUri(entityName2),subscribeToUri)
      cloudServiceInTopic.publish(uMessageToByteString(subscribeRequestUMessage2)/*,Metadata.empty.set("subject",entityName)*/)
      val subscribeResponse2 = cloudServiceOut2Topic.expectOneRaw()
      val subscribeResponseUMessage2 = extractUMessage(subscribeResponse2.payload)
      subscribeResponseUMessage2.toKalix.attributes.get.sink.get.entity.get.name shouldBe entityName2

      //check subscribed
      val subscribeTopic = uriSerializer.serialize(subscribeToUri)
      var fetchSubscribersRes = topicsClient.fetchSubscribers(FetchRequest(subscribeTopic)).futureValue
      fetchSubscribersRes.items.size shouldBe 2

      val statusPublishUMessage = buildStatusPublishUMessage(subscribeToUri)
      vehicleInTopic.publish(uMessageToByteString(statusPublishUMessage)/*,Metadata.empty.set("subject",vin)*/)
      val routedStatusPublish = cloudServiceOut1Topic.expectOneRaw()
      val routedStatusPublishUMessage = extractUMessage(routedStatusPublish.payload)
      //TODO entity and resource id is lost in mapping kalix to eclipse
      //routedStatusPublishUMessage.getAttributes.getSource shouldBe subscribeToUri
      routedStatusPublishUMessage.getAttributes.getSource.getAuthority.getName shouldBe subscribeToUri.getAuthority.getName

      val routedStatusPublish2 = cloudServiceOut2Topic.expectOneRaw()
      val routedStatusPublishUMessage2 = extractUMessage(routedStatusPublish2.payload)
      //TODO entity and resource id is lost in mapping kalix to eclipse
      //routedStatusPublishUMessage.getAttributes.getSource shouldBe subscribeToUri
      routedStatusPublishUMessage2.getAttributes.getSource.getAuthority.getName shouldBe subscribeToUri.getAuthority.getName


    }
  }

  override def afterAll(): Unit = {
    testKit.stop()
    super.afterAll()
  }
  private def ceProtoFormat = BaseDispatcherAction.ceProtoFormat
  private def uriSerializer = BaseDispatcherAction.uriSerializer
  val log = LoggerFactory.getLogger("IntegrationSpec")

  private def extractUMessage(bs: ByteString): UMessage = {
    val outerCloudEvent = ceProtoFormat.deserialize(bs.toByteArray)
    KalixUCloudEvent.toMessage(outerCloudEvent)
  }
  private def buildSubscriberUri(subscriberEntityName: String): UUri =
    UUri.newBuilder
      .setAuthority(UMessageHelper.buildUAuthorityForCloudService())
      .setEntity(UEntity.newBuilder().setName(subscriberEntityName).setId(1).build())
      .build()

  private def buildDoorSubscribeToUri(vin: String): UUri =
    UUri.newBuilder
      .setAuthority(UMessageHelper.buildUAuthorityForVehicle(vin))
      .setEntity(UEntity.newBuilder().setName("body.access").setId(1).build())
      .setResource(UResource.newBuilder().setName("door").setInstance("front_left").setId(1).setMessage("Door"))
      .build()


  private def buildStatusPublishUMessage(sourceUri: UUri): UMessage = {
    val status = buildStatusResponse(true)
    val uPayload = UPayload.newBuilder.setFormat(UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF).setValue(status.getMessageBytes).build
    UMessageHelper.buildPublishUMessage(sourceUri,uPayload)
  }

  private def buildStatusResponse(isSuccess: Boolean): Status = {
    val code = if (isSuccess) Code.OK else Code.ABORTED;
    val status = Status.newBuilder
      .setCode(code.getNumber)
      .setMessage(s"Code: ${code}")
      .build
    status
  }

  private def uMessageToByteString(uMessage: UMessage) = {
    val bs = ByteString.copyFrom(ceProtoFormat.serialize(UCloudEvent.fromMessage(uMessage)))
    log.info(s"input message: ${new String(bs.toByteArray,StandardCharsets.UTF_8)}")
    bs
  }
}
