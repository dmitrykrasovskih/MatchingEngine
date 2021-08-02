package com.lykke.matching.engine.messages.wrappers

import com.google.protobuf.StringValue
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.Timer
import java.io.IOException
import java.math.BigDecimal
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
        errorMessage: String? = null,
        version: Long? = null,
        price: BigDecimal? = null,
        isStraight: Boolean? = null,
        volume: BigDecimal? = null,
        oppositeVolume: BigDecimal? = null
    ) {
        marketOrderTimer?.record(System.nanoTime() - startTimestamp, TimeUnit.NANOSECONDS)
        val responseBuilder = GrpcIncomingMessages.MarketOrderResponse.newBuilder()
        responseBuilder.id = id
        responseBuilder.status = GrpcIncomingMessages.Status.forNumber(status.type)
        if (errorMessage != null) {
            responseBuilder.statusReason = StringValue.of(errorMessage)
        }

        responseBuilder.messageId = StringValue.of(messageId)

        if (price != null) {
            responseBuilder.price = StringValue.of(price.toPlainString())
            if (isStraight != null && isStraight) {
                responseBuilder.baseVolume = StringValue.of(volume!!.toPlainString())
                responseBuilder.quotingVolume = StringValue.of(oppositeVolume!!.toPlainString())
            } else {
                responseBuilder.baseVolume = StringValue.of(oppositeVolume!!.toPlainString())
                responseBuilder.quotingVolume = StringValue.of(volume!!.toPlainString())
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