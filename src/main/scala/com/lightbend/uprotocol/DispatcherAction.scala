package com.lightbend.uprotocol

import cloudevent.factory.KalixUCloudEvent
import model.Implicits._
import com.google.protobuf.empty.Empty
import com.google.protobuf.wrappers.BytesValue
import com.lightbend.uprotocol.model.cloudevent.Event
import io.cloudevents.protobuf.{ProtobufFormat, WormholeProtoSerializer}
import kalix.scalasdk.DeferredCall
//import kalix.scalasdk.action.Action
import kalix.scalasdk.action.Action.Effect
import kalix.scalasdk.action.ActionCreationContext

import org.eclipse.{ uprotocol => eclipseUprotocol }
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

import java.net.URI
import com.google.protobuf.ByteString

import com.lightbend.uprotocol.BaseDispatcherAction
// This class was initially generated based on the .proto definition by Kalix tooling.
//
// As long as this file exists it will not be overwritten: you can maintain it yourself,
// or delete it so it is regenerated as needed.

class DispatcherAction(creationContext: ActionCreationContext) extends AbstractDispatcherAction with BaseDispatcherAction {




  private def thisAction = components.dispatcherAction

  override def sendToUEntityOut(publishUMessage: PublishUMessage, outputId: Int): DeferredCall[PublishUMessage, BytesValue] = {
    if(outputId == 1)
      thisAction.cloudServiceOut1(publishUMessage)
    else
      thisAction.cloudServiceOut2(publishUMessage)
  }

  override def sendToVehicleOut(publishUMessage: PublishUMessage): DeferredCall[PublishUMessage, BytesValue] = thisAction.vehicleOut(publishUMessage)

  override def vehicleIn(bytesValue: BytesValue): Effect[Empty] = in(bytesValue)

  override def vehicleOut(publishUMessage: PublishUMessage): Effect[BytesValue] = out(publishUMessage)

  override def cloudServiceIn(bytesValue: BytesValue): Effect[Empty] =in(bytesValue)

  override def cloudServiceOut1(publishUMessage: PublishUMessage): Effect[BytesValue] = out(publishUMessage)

  override def cloudServiceOut2(publishUMessage: PublishUMessage): Effect[BytesValue] = out(publishUMessage)
}

