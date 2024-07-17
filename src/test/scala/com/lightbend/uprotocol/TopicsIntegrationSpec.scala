package com.lightbend.uprotocol

import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import com.lightbend.uprotocol.model.common.{UAuthority, UEntity, UResource, UUri}
import com.lightbend.uprotocol.model.subscription.{SubscribeAttributes, SubscriberInfo}
import kalix.scalasdk.testkit.KalixTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpec
import com.lightbend.uprotocol.model.Implicits
import org.eclipse.uprotocol.uri.serializer.LongUriSerializer
import org.slf4j.LoggerFactory

import java.time.Instant

// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class TopicsIntegrationSpec
  extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit private val patience: PatienceConfig =
    PatienceConfig(Span(5, Seconds), Span(500, Millis))

  private val testKit = KalixTestKit(Main.createKalix()).start()

  private val client = testKit.getGrpcClient(classOf[Topics])

  "Topics" must {

    "process 3 subscribe, process 1 unsubscribe and return only the remaining subscribers" in {
      // Send 3 Subscribe requests
      client.subscribe(buildSubscribeRequest("1")).futureValue
      client.subscribe(buildSubscribeRequest("2")).futureValue
      client.subscribe(buildSubscribeRequest("3")).futureValue

      buildSubscribeRequest("1")
      val resultAfterSubscribe = client.fetchSubscribers(buildFetchRequest()).futureValue
      resultAfterSubscribe.items.size shouldBe 3
      resultAfterSubscribe.items.exists(item => item.subscriber.get.uri.get.resource.get.instance.get == "1") shouldBe true
      resultAfterSubscribe.items.exists(item => item.subscriber.get.uri.get.resource.get.instance.get == "2") shouldBe true
      resultAfterSubscribe.items.exists(item => item.subscriber.get.uri.get.resource.get.instance.get == "3") shouldBe true

      // Send 1 Unsubscribe request
      client.unsubscribe(buildUnsubscribeRequest("1")).futureValue
      val resultAfterUnsubscribe = client.fetchSubscribers(buildFetchRequest()).futureValue
      resultAfterUnsubscribe.items.size shouldBe 2
      resultAfterUnsubscribe.items.exists(item => item.subscriber.get.uri.get.resource.get.instance.get == "2") shouldBe true
      resultAfterUnsubscribe.items.exists(item => item.subscriber.get.uri.get.resource.get.instance.get == "3") shouldBe true
    }
  }

  override def afterAll(): Unit = {
    testKit.stop()
    super.afterAll()
  }

  private def buildSubscribeRequest(id: String): RequestToSubscribe = {
    // Build subscriber uuri
    val authoritySubscriber = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entitySubscriber = Some(UEntity("body.access", None, Some(1), None))
    val resourceSubscriber = Some(UResource("door", Some(id), Some("Door"), None))
    val uuriSubscriber = Some(UUri(authoritySubscriber, entitySubscriber, resourceSubscriber))
    // Build topic uuri
    val authorityTopic = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entityTopic = Some(UEntity("body.access", None, Some(1), None))
    val resourceTopic = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuriTopic = UUri(authorityTopic, entityTopic, resourceTopic)
    // Build RequestToSubscribe
    val subscriber = Some(SubscriberInfo(uuriSubscriber, Seq.empty))
    val attributes = Some(SubscribeAttributes(Some(Timestamp(Instant.now())), Seq.empty, None))

    RequestToSubscribe(toString(uuriTopic), subscriber, attributes)
  }

  private def buildUnsubscribeRequest(id: String): RequestToUnsubscribe = {
    // Build subscriber uuri
    val authoritySubscriber = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entitySubscriber = Some(UEntity("body.access", None, Some(1), None))
    val resourceSubscriber = Some(UResource("door", Some(id), Some("Door"), None))
    val uuriSubscriber = Some(UUri(authoritySubscriber, entitySubscriber, resourceSubscriber))
    // Build topic uuri
    val authorityTopic = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entityTopic = Some(UEntity("body.access", None, Some(1), None))
    val resourceTopic = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuriTopic = UUri(authorityTopic, entityTopic, resourceTopic)
    // Build RequestToUnsubscribe
    val subscriber = Some(SubscriberInfo(uuriSubscriber, Seq.empty))
    RequestToUnsubscribe(toString(uuriTopic), subscriber)
  }

  private def buildFetchRequest() = {
    // Build uUri
    val authority = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entity = Some(UEntity("body.access", None, Some(1), None))
    val resource = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuri = UUri(authority, entity, resource)
    // Build FetchRequest
    FetchRequest(toString(uuri), None)
  }

  private def toString(uuri: UUri): String = {
    LongUriSerializer.instance().serialize(Implicits.KalixUUriOps(uuri).toEclipse)
  }
}
