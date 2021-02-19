package com.lykke.matching.engine.services

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import org.apache.logging.log4j.LogManager
import org.springframework.stereotype.Service
import java.util.*

@Service
class LimitOrderMassCancelService(
    private val genericLimitOrderService: GenericLimitOrderService,
    private val genericStopLimitOrderService: GenericStopLimitOrderService,
    private val limitOrdersCancelExecutor: LimitOrdersCancelExecutor
) : AbstractService {
    companion object {
        private val LOGGER = LogManager.getLogger(LimitOrderMassCancelService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val now = Date()
        val messageWrapper = genericMessageWrapper as LimitOrderMassCancelMessageWrapper
        val context = messageWrapper.context
        LOGGER.debug("Got mass limit order cancel request id: ${context!!.uid}, clientId: ${context.clientId}, assetPairId: ${context.assetPairId}, isBuy: ${context.isBuy}")

        val updateSuccessful = limitOrdersCancelExecutor.cancelOrdersAndApply(
            CancelRequest(
                genericLimitOrderService.searchOrders(context.clientId, context.assetPairId, context.isBuy),
                genericStopLimitOrderService.searchOrders(context.clientId, context.assetPairId, context.isBuy),
                context.messageId,
                context.uid,
                context.messageType,
                now,
                context.processedMessage,
                messageWrapper,
                LOGGER
            )
        )

        if (updateSuccessful) {
            messageWrapper.writeResponse(MessageStatus.OK)
        } else {
            val message = "Unable to save result"
            messageWrapper.writeResponse(MessageStatus.RUNTIME, message)
            LOGGER.info("$message for operation ${context.messageId}")
        }
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as LimitOrderMassCancelMessageWrapper
        messageWrapper.writeResponse(status)
    }
}