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
        builder.setMessageType(mapMessageType(messageType))
            .setSequenceNumber(sequenceNumber)
            .setMessageId(messageId)
            .setRequestId(requestId)
            .setVersion(version)
            .setTimestamp(timestamp.createProtobufTimestampBuilder())
            .eventType = eventType
        return builder
    }

    private fun mapMessageType(messageType: MessageType): OutgoingMessages.Header.MessageType {
        return when (messageType) {
            MessageType.UNKNOWN_MESSAGE_TYPE -> OutgoingMessages.Header.MessageType.UNKNOWN_MESSAGE_TYPE
            MessageType.CASH_IN -> OutgoingMessages.Header.MessageType.CASH_IN
            MessageType.CASH_OUT -> OutgoingMessages.Header.MessageType.CASH_OUT
            MessageType.CASH_TRANSFER -> OutgoingMessages.Header.MessageType.CASH_TRANSFER
            MessageType.ORDER -> OutgoingMessages.Header.MessageType.ORDER
            MessageType.RESERVED_CASH -> OutgoingMessages.Header.MessageType.RESERVED_CASH
            MessageType.CASH_SWAP -> OutgoingMessages.Header.MessageType.CASH_SWAP
        }
    }
}