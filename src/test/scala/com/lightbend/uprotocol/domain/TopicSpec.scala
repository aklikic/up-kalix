package com.lightbend.uprotocol.domain

import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import com.lightbend.uprotocol
import com.lightbend.uprotocol.{RequestToSubscribe, RequestToUnsubscribe}
import com.lightbend.uprotocol.model.Implicits
import com.lightbend.uprotocol.model.common._
import com.lightbend.uprotocol.model.subscription._
import org.eclipse.uprotocol.uri.serializer.LongUriSerializer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class TopicSpec extends AnyWordSpec with Matchers {
  "The Topic" should {

    "successfully process a Subscribe command" in {
      val outputId = 1
      val testKit = TopicTestKit(new Topic(_, () => Instant.now()))
      val request = buildSubscribeRequest("1",outputId)

      // Execute the SubscribeRequest
      val result = testKit.subscribe(request)
      // Verify the emitted events
      val actualEvent = result.nextEvent[Subscribed]
      actualEvent.subscriber.get.uri.get.resource.get.instance.get shouldBe "1"
      actualEvent.outputId shouldBe outputId
      // Verify the final state after applying the events
      testKit.currentState.subscribers.keys.exists(_.equals("//VCU/body.access/1/door.1#Door")) shouldBe true
      testKit.currentState.subscribers.get("//VCU/body.access/1/door.1#Door").get.outputId shouldBe outputId
      // Verify the reply
      result.reply.status.get.state shouldBe SubscriptionStatus.State.SUBSCRIBED
    }

    "successfully process multiple Subscribe commands" in {
      val outputId = 1
      val testKit = TopicTestKit(new Topic(_, () => Instant.now()))
      val request1 = buildSubscribeRequest("1", outputId)
      val request2 = buildSubscribeRequest("2", outputId)
      val request3 = buildSubscribeRequest("3", outputId)

      // Execute the SubscribeRequest
      val result1 = testKit.subscribe(request1)
      val result2 = testKit.subscribe(request1)
      val result3 = testKit.subscribe(request1)

      // Verify the reply
      result1.reply.status.get.state shouldBe SubscriptionStatus.State.SUBSCRIBED
      result2.reply.status.get.state shouldBe SubscriptionStatus.State.SUBSCRIBED
      result3.reply.status.get.state shouldBe SubscriptionStatus.State.SUBSCRIBED
    }

    "successfully process a Unsubscribe command" in {
      val outputId = 1
      val testKit = TopicTestKit(new Topic(_, () => Instant.now()))
      val requestToSubscribe = buildSubscribeRequest("1", outputId)
      val requestToUnsubscribe = buildUnsubscribeRequest("1")

      // Execute the SubscribeRequest
      val subscribeResult = testKit.subscribe(requestToSubscribe)
      // Verify the reply
      subscribeResult.reply.status.get.state shouldBe SubscriptionStatus.State.SUBSCRIBED
      // Execute the UnSubscribeRequest
      val result = testKit.unsubscribe(requestToUnsubscribe)
      // Verify the emitted events
      val actualEvent = result.nextEvent[Unsubscribed]
      actualEvent.subscriber.get.uri.get.resource.get.instance.get shouldBe "1"
      // Verify the final state after applying the events
      testKit.currentState.subscribers.isEmpty shouldBe true
      // Verify the reply
      result.reply shouldBe Empty()
    }

    "successfully process a FetchSubscribers command by returning all Subscribers for a topic" in {
      val outputId = 1
      val testKit = TopicTestKit(new Topic(_, () => Instant.now()))
      val requestToSubscribe = buildSubscribeRequest("1", outputId)
      val fetchSubscriptionsRequest = buildFetchSubscriptionsRequestForTopic()

      // Execute the SubscribeRequest
      testKit.subscribe(requestToSubscribe)
      // Execute the FetchSubscribersRequest
      val fetchSubscribersResult = testKit.fetchSubscribers(fetchSubscriptionsRequest)
      // Verify the emitted events
      fetchSubscribersResult.didEmitEvents shouldBe false
      // Verify the final state after applying the events
      testKit.currentState.subscribers.keys.isEmpty shouldBe false
      // Verify the reply
      fetchSubscribersResult.reply.items.size shouldBe 1
      fetchSubscribersResult.reply.items.head.subscriber.get.uri.get.resource.get.name shouldBe "door"
    }

    "successfully process a FetchSubscribers for a topic with multiple subscribers" in {
      val outputId = 1
      val testKit = TopicTestKit(new Topic(_, () => Instant.now()))
      val requestToSubscribe1 = buildSubscribeRequest("1", outputId)
      val requestToSubscribe2 = buildSubscribeRequest("2", outputId)
      val fetchSubscriptionsRequest = buildFetchSubscriptionsRequestForTopic()

      // Execute the SubscribeRequest
      testKit.subscribe(requestToSubscribe1)
      testKit.subscribe(requestToSubscribe2)
      // Execute the FetchSubscribersRequest
      val fetchSubscribersResult = testKit.fetchSubscribers(fetchSubscriptionsRequest)
      // Verify the emitted events
      fetchSubscribersResult.didEmitEvents shouldBe false
      // Verify the final state after applying the events
      testKit.currentState.subscribers.keys.isEmpty shouldBe false
      // Verify the reply
      fetchSubscribersResult.reply.items.size shouldBe 2
    }

    "successfully process a FetchSubscribers command by returning all topics a subscriber is subscribed to" in {
      // This feature is not implemented yet.
      pending
    }
  }

  private def buildSubscribeRequest(id: String, outputId: Int) = {
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
    RequestToSubscribe(toString(uuriTopic), subscriber, attributes, outputId)
  }

  private def buildUnsubscribeRequest(id: String) = {
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

  private def buildFetchSubscriptionsRequestForTopic() = {
    // Build uUri
    val authority = Some(UAuthority(Some("VCU"), UAuthority.Number.Id(ByteString.copyFromUtf8("5GAEVCKW1MJ100751"))))
    val entity = Some(UEntity("body.access", None, Some(1), None))
    val resource = Some(UResource("door", Some("front_left"), Some("Door"), None))
    val uuri = UUri(authority, entity, resource)
    // Build FetchRequest
    uprotocol.FetchRequest(toString(uuri), None)
  }

  private def toString(uuri: UUri): String = {
    LongUriSerializer.instance().serialize(Implicits.KalixUUriOps(uuri).toEclipse)
  }

}

