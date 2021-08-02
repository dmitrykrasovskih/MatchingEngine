package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.LimitOrderCancelMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.order.process.common.CancelRequest
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import com.lykke.matching.engine.services.validators.business.LimitOrderCancelOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*
import java.util.stream.Collectors

@Service
class LimitOrderCancelService(
    private val genericLimitOrderService: GenericLimitOrderService,
    private val genericStopLimitOrderService: GenericStopLimitOrderService,
    private val validator: LimitOrderCancelOperationBusinessValidator,
    private val limitOrdersCancelExecutor: LimitOrdersCancelExecutor
) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(LimitOrderCancelService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val now = Date()
        val messageWrapper = genericMessageWrapper as LimitOrderCancelMessageWrapper
        val context = messageWrapper.context

        LOGGER.debug("Got limit order cancel request (messageId: ${context!!.messageId}, id: ${context.uid}, orders: ${context.limitOrderIds})")
        val ordersByType = getLimitOrderTypeToLimitOrders(context.limitOrderIds)

        try {
            validator.performValidation(ordersByType, context)
        } catch (e: ValidationException) {
            LOGGER.info("Business validation failed: ${context.messageId}, details: ${e.message}")
            messageWrapper.writeResponse(MessageStatusUtils.toMessageStatus(e.validationType))
            return
        }

        val updateSuccessful = limitOrdersCancelExecutor.cancelOrdersAndApply(
            CancelRequest(
                ordersByType[LimitOrderType.LIMIT] ?: emptyList(),
                ordersByType[LimitOrderType.STOP_LIMIT] ?: emptyList(),
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
        val messageWrapper = genericMessageWrapper as LimitOrderCancelMessageWrapper
        messageWrapper.writeResponse(status)
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, processedMessage: ProcessedMessage) {
        val messageWrapper = genericMessageWrapper as LimitOrderCancelMessageWrapper
        messageWrapper.writeResponse(
            processedMessage.status ?: MessageStatus.DUPLICATE
        )
    }

    private fun getLimitOrderTypeToLimitOrders(orderIds: Set<String>): Map<LimitOrderType, List<LimitOrder>> {
        return orderIds.stream()
            .map(::getOrder)
            .filter { limitOrder -> limitOrder != null }
            .map { t -> t!! }
            .collect(Collectors.groupingBy { limitOrder: LimitOrder -> limitOrder.type ?: LimitOrderType.LIMIT })
    }

    private fun getOrder(orderId: String): LimitOrder? {
        return genericLimitOrderService.getOrder(orderId) ?: genericStopLimitOrderService.getOrder(orderId)
    }
}
