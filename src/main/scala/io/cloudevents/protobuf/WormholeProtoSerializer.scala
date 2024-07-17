package io.cloudevents.protobuf

// Because io.cloudevents.protobuf.ProtoSerializer is package private...
object WormholeProtoSerializer {
  def toProto(ce: io.cloudevents.CloudEvent): io.cloudevents.v1.proto.CloudEvent =
    ProtoSerializer.toProto(ce)
}
