package com.lykke.matching.engine.services

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.socket.MultiLimitOrderCancelMessageWrapper
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*

@Service
class MultiLimitOrderCancelService(
    private val limitOrderService: GenericLimitOrderService,
    private val limitOrdersCancelExecutor: LimitOrdersCancelExecutor,
    private val applicationSettingsHolder: ApplicationSettingsHolder
) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderCancelService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val messageWrapper = genericMessageWrapper as MultiLimitOrderCancelMessageWrapper
        val message = messageWrapper.parsedMessage
        messageWrapper.processedMessage = if (applicationSettingsHolder.isTrustedClient(message.walletId))
            null
        else
            ProcessedMessage(messageWrapper.type.type, Date().time, messageWrapper.messageId)
        LOGGER.debug(
            "Got multi limit order cancel " +
                    "message id: ${messageWrapper.messageId}, id: ${message.id}, ${message.walletId}, " +
                    "assetPair: ${message.assetPairId}, isBuy: ${message.isBuy}"
        )

        val now = Date()
        val ordersToCancel = limitOrderService.searchOrders(message.walletId, message.assetPairId, message.isBuy)
        if (ordersToCancel.isEmpty()) {
            messageWrapper.writeResponse(MessageStatus.OK)
            return
        }

        val updateSuccessful = limitOrdersCancelExecutor.cancelOrdersAndApply(
            CancelRequest(
                ordersToCancel,
                emptyList(),
                messageWrapper.messageId,
                message.id,
                MessageType.MULTI_LIMIT_ORDER_CANCEL,
                now,
                messageWrapper.processedMessage,
                messageWrapper,
                LOGGER
            )
        )

        if (updateSuccessful) {
            messageWrapper.writeResponse(MessageStatus.OK)
            LOGGER.debug("Multi limit order cancel id: ${message.id}, client ${message.walletId}, assetPair: ${message.assetPairId}, isBuy: ${message.isBuy} processed")
        } else {
            val errorMessage = "Unable to save result"
            messageWrapper.writeResponse(MessageStatus.RUNTIME, errorMessage)
            LOGGER.info("$errorMessage for operation ${messageWrapper.id}")
        }
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as MultiLimitOrderCancelMessageWrapper
        messageWrapper.writeResponse(status)
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, processedMessage: ProcessedMessage) {
        val messageWrapper = genericMessageWrapper as MultiLimitOrderCancelMessageWrapper
        messageWrapper.writeResponse(
            processedMessage.status ?: MessageStatus.DUPLICATE
        )
    }
}