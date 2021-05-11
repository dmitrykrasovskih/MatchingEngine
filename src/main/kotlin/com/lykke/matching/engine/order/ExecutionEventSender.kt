package com.lykke.matching.engine.order

import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.stereotype.Component

@Component
class ExecutionEventSender(
    private val messageSender: MessageSender,
    private val genericLimitOrderService: GenericLimitOrderService,
) {

    fun sendEvents(
        executionContext: ExecutionContext,
        sequenceNumbers: SequenceNumbersWrapper
    ) {
        executionContext.orderBooksHolder.tradeInfoList.forEach {
            genericLimitOrderService.putTradeInfo(it)
        }

        val trustedClientsLimitOrdersWithTrades =
            executionContext.getTrustedClientsLimitOrdersWithTrades().toMutableList()
        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades)) {
            messageSender.sendTrustedClientsMessage(
                EventFactory.createTrustedClientsExecutionEvent(
                    sequenceNumbers.trustedClientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    trustedClientsLimitOrdersWithTrades
                )
            )
        }

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades)) {
            messageSender.sendMessage(
                EventFactory.createExecutionEvent(
                    sequenceNumbers.clientsSequenceNumber!!,
                    executionContext.messageId,
                    executionContext.requestId,
                    executionContext.date,
                    executionContext.messageType,
                    executionContext.walletOperationsProcessor.getClientBalanceUpdates(),
                    clientsLimitOrdersWithTrades,
                    executionContext.marketOrderWithTrades
                )
            )
        }
    }
}