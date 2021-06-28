package com.lykke.matching.engine.messages.wrappers

import com.google.protobuf.StringValue
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.OrderStatus
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.Timer
import java.io.IOException
import java.util.concurrent.TimeUnit

class MarketOrderMessageWrapper(
    val parsedMessage: GrpcIncomingMessages.MarketOrder,
    private val callback: StreamObserver<GrpcIncomingMessages.MarketOrderResponse>,
    private val marketOrderTimer: Timer? = null,
    private val closeStream: Boolean = false
) : MessageWrapper(
    MessageType.MARKET_ORDER,
    parsedMessage.id,
    if (parsedMessage.hasMessageId()) parsedMessage.messageId.value else parsedMessage.id
) {

    fun writeResponse(
        status: MessageStatus,
        order: MarketOrder? = null,
        errorMessage: String? = null,
        version: Long? = null
    ) {
        marketOrderTimer?.record(System.nanoTime() - startTimestamp, TimeUnit.NANOSECONDS)
        val responseBuilder = GrpcIncomingMessages.MarketOrderResponse.newBuilder()
        responseBuilder.id = id
        responseBuilder.status = GrpcIncomingMessages.Status.forNumber(status.type)
        if (errorMessage != null) {
            responseBuilder.statusReason = StringValue.of(errorMessage)
        }

        responseBuilder.messageId = StringValue.of(messageId)

        if (order != null && order.status == OrderStatus.Matched.name) {
            if (order.price != null) {
                responseBuilder.price = StringValue.of(order.price!!.toPlainString())
            }
            if (order.isStraight()) {
                responseBuilder.baseVolume = StringValue.of(order.volume.toPlainString())
                responseBuilder.quotingVolume = StringValue.of(order.oppositeVolume.toPlainString())
            } else {
                responseBuilder.baseVolume = StringValue.of(order.oppositeVolume.toPlainString())
                responseBuilder.quotingVolume = StringValue.of(order.volume.toPlainString())
            }
        }

        if (version != null) {
            responseBuilder.walletVersion = version
        }

        writeClientResponse(responseBuilder.build())
    }

    private fun writeClientResponse(response: GrpcIncomingMessages.MarketOrderResponse) {
        try {
            val start = System.nanoTime()
            callback.onNext(response)
            writeResponseTime = System.nanoTime() - start
            if (closeStream) {
                callback.onCompleted()
            }
        } catch (exception: IOException) {
            LOGGER.error("Unable to write for message with id $messageId response: ${exception.message}", exception)
        }
    }
}