package com.lightbend.uprotocol

import com.google.protobuf.timestamp.Timestamp

import java.time.Instant

object ProtoInstant {
  def fromProto(proto: Timestamp): Instant = Instant.ofEpochSecond(proto.seconds, proto.nanos)
  def toProto(instant: Instant): Timestamp = Timestamp(instant.getEpochSecond, instant.getNano)
}
