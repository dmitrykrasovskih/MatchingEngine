package com.lykke.matching.engine.messages.wrappers

import com.google.protobuf.StringValue
import com.lykke.matching.engine.daos.context.CashSwapContext
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import java.io.IOException

class CashSwapOperationMessageWrapper(
    var parsedMessage: GrpcIncomingMessages.CashSwapOperation,
    private val callback: StreamObserver<GrpcIncomingMessages.CashSwapOperationResponse>,
    private val closeStream: Boolean = false,
    var context: CashSwapContext? = null
) : MessageWrapper(
    MessageType.CASH_SWAP_OPERATION,
    parsedMessage.id,
    if (parsedMessage.hasMessageId()) parsedMessage.messageId.value else parsedMessage.id
) {

    @Suppress("DuplicatedCode")
    fun writeResponse(operationId: String?, status: MessageStatus, errorMessage: String? = null) {
        val responseBuilder = GrpcIncomingMessages.CashSwapOperationResponse.newBuilder()
        responseBuilder.id = id
        responseBuilder.status = GrpcIncomingMessages.Status.forNumber(status.type)
        if (errorMessage != null) {
            responseBuilder.statusReason = StringValue.of(errorMessage)
        }
        if (operationId != null) {
            responseBuilder.matchingEngineId = StringValue.of(operationId)
        }
        responseBuilder.messageId = StringValue.of(messageId)

        writeClientResponse(responseBuilder.build())
    }

    private fun writeClientResponse(response: GrpcIncomingMessages.CashSwapOperationResponse) {
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