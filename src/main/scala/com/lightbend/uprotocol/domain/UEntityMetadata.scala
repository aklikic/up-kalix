package com.lightbend.uprotocol.domain

import com.google.protobuf.empty.Empty
import com.lightbend.uprotocol
import com.lightbend.uprotocol.FetchUEntityMetadataResponse
import io.grpc.Status
import kalix.scalasdk.eventsourcedentity.EventSourcedEntity
import kalix.scalasdk.eventsourcedentity.EventSourcedEntityContext

// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class UEntityMetadata(context: EventSourcedEntityContext) extends AbstractUEntityMetadata {
  override def emptyState: UEntityMetadataState = UEntityMetadataState()

  override def addUEntityMetadata(currentState: UEntityMetadataState, addUEntityMetadataRequest: uprotocol.AddUEntityMetadataRequest): EventSourcedEntity.Effect[Empty] = {
    if(1 to 3 contains addUEntityMetadataRequest.outputId)
      effects.emitEvent(UEntityMetadataAdded(addUEntityMetadataRequest.outputId))
        .thenReply(_ => Empty.defaultInstance)
    else
      effects.error("messageBrokerTopicPairId must be between 1 and 3 ", Status.Code.INVALID_ARGUMENT)
  }

  override def fetchUEntityMetadata(currentState: UEntityMetadataState, fetchUEntityMetadataRequest: uprotocol.FetchUEntityMetadataRequest): EventSourcedEntity.Effect[uprotocol.FetchUEntityMetadataResponse] =
    effects.reply(FetchUEntityMetadataResponse(currentState.outputId))

  override def uEntityMetadataAdded(currentState: UEntityMetadataState, uEntityMetadataAdded: UEntityMetadataAdded): UEntityMetadataState =
    UEntityMetadataState(uEntityMetadataAdded.outputId)

}
