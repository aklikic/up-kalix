package com.lightbend.uprotocol

import akka.actor.ActorSystem
import com.google.protobuf.empty.Empty
import kalix.scalasdk.testkit.KalixTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import org.scalatest.wordspec.AnyWordSpec

// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class UEntityMetadatasIntegrationSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit private val patience: PatienceConfig =
    PatienceConfig(Span(5, Seconds), Span(500, Millis))

  private val testKit = KalixTestKit(Main.createKalix()).start()

  private val client = testKit.getGrpcClient(classOf[UEntityMetadatas])

  "UEntityMetadatas" must {

    "have entity metadata and fetch it" in {
      val entityName = "cs1"
      val outputId = 1
      client.addUEntityMetadata(AddUEntityMetadataRequest(entityName,outputId)).futureValue
      val resultAfterGet = client.fetchUEntityMetadata(FetchUEntityMetadataRequest(entityName)).futureValue
      resultAfterGet.outputId shouldBe outputId
    }

  }

  override def afterAll(): Unit = {
    testKit.stop()
    super.afterAll()
  }
}
