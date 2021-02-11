package com.lykke.matching.engine.messages.wrappers

import com.google.protobuf.StringValue
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.order.process.ProcessedOrder
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import java.io.IOException

class MultiLimitOrderMessageWrapper(
    var parsedMessage: GrpcIncomingMessages.MultiLimitOrder,
    private val callback: StreamObserver<GrpcIncomingMessages.MultiLimitOrderResponse>,
    private val closeStream: Boolean = false
) : MessageWrapper(
    MessageType.MULTI_LIMIT_ORDER,
    parsedMessage.id,
    if (parsedMessage.hasMessageId()) parsedMessage.messageId.value else parsedMessage.id
) {

    fun writeResponse(
        status: MessageStatus,
        errorMessage: String? = null,
        processedOrders: List<ProcessedOrder>? = null,
        version: Long? = null
    ) {
        val responseBuilder = GrpcIncomingMessages.MultiLimitOrderResponse.newBuilder()
        responseBuilder.id = id
        responseBuilder.status = GrpcIncomingMessages.Status.forNumber(status.type)
        if (errorMessage != null) {
            responseBuilder.statusReason = StringValue.of(errorMessage)
        }

        responseBuilder.messageId = StringValue.of(messageId)

        if (version != null) {
            responseBuilder.walletVersion = version
        }

        processedOrders?.forEach { processedOrder ->
            val order = processedOrder.order
            val statusBuilder = GrpcIncomingMessages.MultiLimitOrderResponse.OrderStatus.newBuilder()
                .setId(order.externalId)
                .setMatchingEngineId(StringValue.of(order.id))
                .setStatus(GrpcIncomingMessages.Status.forNumber(MessageStatusUtils.toMessageStatus(order.status).type))
                .setVolume(order.volume.toPlainString())
                .setPrice(order.price.toPlainString())
            processedOrder.reason?.let { statusBuilder.statusReason = StringValue.of(processedOrder.reason) }
            responseBuilder.addStatuses(statusBuilder)
        }

        writeClientResponse(responseBuilder.build())
    }

    private fun writeClientResponse(response: GrpcIncomingMessages.MultiLimitOrderResponse) {
        try {
            val start = System.nanoTime()
            callback.onNext(response)
            writeResponseTime = System.nanoTime() - start
            if (closeStream) {
                callback.onCompleted()
            }
        } catch (exception: IOException) {
            LOGGER.error("Unable to write for message with id $messageId response: ${exception.message}", exception)
            METRICS_LOGGER.logError("Unable to write response", exception)
        }
    }
}