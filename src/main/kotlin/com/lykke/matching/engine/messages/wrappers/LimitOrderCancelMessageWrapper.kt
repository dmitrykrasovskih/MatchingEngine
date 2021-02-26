package com.lykke.matching.engine.messages.wrappers

import com.google.protobuf.StringValue
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.Timer
import java.io.IOException
import java.util.concurrent.TimeUnit

class LimitOrderCancelMessageWrapper(
    var parsedMessage: GrpcIncomingMessages.LimitOrderCancel,
    private val callback: StreamObserver<GrpcIncomingMessages.LimitOrderCancelResponse>?,
    private val cancelOrderTimer: Timer? = null,
    private val closeStream: Boolean = false,
    var context: LimitOrderCancelOperationContext? = null
) : MessageWrapper(
    MessageType.LIMIT_ORDER_CANCEL,
    parsedMessage.id,
    if (parsedMessage.hasMessageId()) parsedMessage.messageId.value else parsedMessage.id
) {

    fun writeResponse(
        status: MessageStatus,
        errorMessage: String? = null,
        orderId: String? = null,
        version: Long? = null
    ) {
        cancelOrderTimer?.record(System.nanoTime() - startTimestamp, TimeUnit.NANOSECONDS)
        val responseBuilder = GrpcIncomingMessages.LimitOrderCancelResponse.newBuilder()
        responseBuilder.id = id
        responseBuilder.status = GrpcIncomingMessages.Status.forNumber(status.type)
        if (errorMessage != null) {
            responseBuilder.statusReason = StringValue.of(errorMessage)
        }

        if (orderId != null) {
            responseBuilder.matchingEngineId = StringValue.of(orderId)
        }
        responseBuilder.messageId = StringValue.of(messageId)

        if (version != null) {
            responseBuilder.walletVersion = version
        }

        writeClientResponse(responseBuilder.build())
    }

    private fun writeClientResponse(response: GrpcIncomingMessages.LimitOrderCancelResponse) {
        try {
            if (callback != null) {
                val start = System.nanoTime()
                callback.onNext(response)
                writeResponseTime = System.nanoTime() - start
                if (closeStream) {
                    callback.onCompleted()
                }
            }
        } catch (exception: IOException) {
            LOGGER.error("Unable to write for message with id $messageId response: ${exception.message}", exception)
            METRICS_LOGGER.logError("Unable to write response", exception)
        }
    }
}