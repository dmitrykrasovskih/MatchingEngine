package com.lykke.matching.engine.utils.proto

import com.google.protobuf.Timestamp
import java.time.Instant
import java.util.Date

fun Date.createProtobufTimestampBuilder(): Timestamp.Builder {
    val instant = this.toInstant()
    return Timestamp.newBuilder()
            .setSeconds(instant.epochSecond)
            .setNanos(instant.nano)
}

fun Timestamp.toDate(): Date {
    val instant = Instant.ofEpochSecond(this.seconds, this.nanos.toLong())
    return Date.from(instant)
}