package com.lightbend.uprotocol.domain

import com.google.protobuf.empty.Empty
import com.lightbend.uprotocol
import com.lightbend.uprotocol.AddUEntityMetadataRequest
import kalix.scalasdk.eventsourcedentity.EventSourcedEntity
import kalix.scalasdk.testkit.EventSourcedResult
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class UEntityMetadataSpec extends AnyWordSpec with Matchers {
  "The UEntityMetadata" should {
    "add new entity metadata" in {
      val entityName = "cs1"
      val outputId = 1
      val testKit = UEntityMetadataTestKit(new UEntityMetadata(_))
      val result = testKit.addUEntityMetadata(AddUEntityMetadataRequest(entityName,outputId))
      val actualEvent = result.nextEvent[UEntityMetadataAdded]
      actualEvent.outputId shouldBe outputId
      testKit.currentState.outputId shouldBe outputId
    }
  }
}
