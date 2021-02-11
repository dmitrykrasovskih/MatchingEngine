package com.lykke.matching.engine.messages.wrappers.socket

import com.google.protobuf.StringValue
import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.myjetwallet.messages.incoming.socket.SocketIncomingMessages
import io.grpc.stub.StreamObserver
import java.io.IOException

class MultiLimitOrderCancelMessageWrapper(
    var parsedMessage: SocketIncomingMessages.MultiLimitOrderCancel,
    private val callback: StreamObserver<SocketIncomingMessages.Response>,
    private val closeStream: Boolean = false,
    var context: LimitOrderMassCancelOperationContext? = null
) : MessageWrapper(
    MessageType.MULTI_LIMIT_ORDER_CANCEL,
    parsedMessage.id,
    if (parsedMessage.hasMessageId()) parsedMessage.messageId.value else parsedMessage.id
) {

    fun writeResponse(
        status: MessageStatus,
        errorMessage: String? = null
    ) {
        val responseBuilder = SocketIncomingMessages.Response.newBuilder()
        responseBuilder.id = id
        responseBuilder.status = SocketIncomingMessages.Status.forNumber(status.type)
        if (errorMessage != null) {
            responseBuilder.statusReason = StringValue.of(errorMessage)
        }

        writeClientResponse(responseBuilder.build())
    }

    private fun writeClientResponse(response: SocketIncomingMessages.Response) {
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