package com.lykke.matching.engine.order

import com.lykke.matching.engine.order.transaction.ExecutionContext
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.utils.event.isThereClientEvent
import com.lykke.matching.engine.utils.event.isThereTrustedClientEvent
import org.springframework.stereotype.Component

@Component
class ExecutionEventSender(
    private val messageSender: MessageSender
) {
    fun generateEvents(
        executionContext: ExecutionContext,
        sequenceNumbers: SequenceNumbersWrapper
    ): EventsHolder {
        val trustedClientsLimitOrdersWithTrades =
            executionContext.getTrustedClientsLimitOrdersWithTrades().toMutableList()
        var trustedClientEvent: Event<*>? = null
        if (isThereTrustedClientEvent(trustedClientsLimitOrdersWithTrades))
            trustedClientEvent = EventFactory.createTrustedClientsExecutionEvent(
                sequenceNumbers.trustedClientsSequenceNumber!!,
                executionContext.messageId,
                executionContext.requestId,
                executionContext.date,
                executionContext.messageType,
                trustedClientsLimitOrdersWithTrades
            )

        val clientsLimitOrdersWithTrades = executionContext.getClientsLimitOrdersWithTrades().toList()
        var clientEvent: Event<*>? = null
        if (isThereClientEvent(clientsLimitOrdersWithTrades, executionContext.marketOrderWithTrades))
            clientEvent = EventFactory.createExecutionEvent(
                sequenceNumbers.clientsSequenceNumber!!,
                executionContext.messageId,
                executionContext.requestId,
                executionContext.date,
                executionContext.messageType,
                executionContext.walletOperationsProcessor.getClientBalanceUpdates(),
                clientsLimitOrdersWithTrades,
                executionContext.marketOrderWithTrades
            )
        return EventsHolder(trustedClientEvent, clientEvent)
    }

    fun sendEvents(
        events: EventsHolder
    ) {
        if (events.trustedClientsEvent != null) {
            messageSender.sendTrustedClientsMessage(events.trustedClientsEvent)
        }
        if (events.clientsEvent != null) {
            messageSender.sendMessage(events.clientsEvent)
        }
    }
}