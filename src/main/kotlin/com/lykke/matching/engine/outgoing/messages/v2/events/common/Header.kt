package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.outgoing.messages.v2.createProtobufTimestampBuilder
import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages
import java.util.*

class Header(
    val messageType: MessageType,
    val sequenceNumber: Long,
    val messageId: String,
    val requestId: String,
    val version: String,
    val timestamp: Date,
    val eventType: String
) : EventPart<OutgoingMessages.Header.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.Header.Builder {
        val builder = OutgoingMessages.Header.newBuilder()
        builder.setMessageType(OutgoingMessages.Header.MessageType.forNumber(messageType.id))
            .setSequenceNumber(sequenceNumber)
            .setMessageId(messageId)
            .setRequestId(requestId)
            .setVersion(version)
            .setTimestamp(timestamp.createProtobufTimestampBuilder())
            .eventType = eventType
        return builder
    }
}