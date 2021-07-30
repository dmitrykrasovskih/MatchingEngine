package com.lykke.matching.engine.order

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.order.transaction.ExecutionContext
import org.springframework.stereotype.Component

@Component
class ExecutionPersistenceService(private val persistenceManager: PersistenceManager) {

    fun persist(
        messageWrapper: MessageWrapper?,
        executionContext: ExecutionContext,
        executionEventSender: ExecutionEventSender,
        sequenceNumber: SequenceNumbersWrapper
    ): EventsHolder? {

        if (messageWrapper?.triedToPersist == true) {
            executionContext.error("There already was attempt to persist data")
            return null
        }

        executionContext.apply()

        val eventsHolder = executionEventSender.generateEvents(executionContext, sequenceNumber)

        val persisted = persistenceManager.persist(
            PersistenceData(
                executionContext.walletOperationsProcessor.persistenceData(),
                executionContext.processedMessage,
                executionContext.orderBooksHolder.getPersistenceData(),
                executionContext.stopOrderBooksHolder.getPersistenceData(),
                sequenceNumber.sequenceNumber,
                eventsHolder
            )
        )

        messageWrapper?.triedToPersist = true
        messageWrapper?.persisted = persisted
        if (!persisted) {
            executionContext.error("Unable to persist result")
        }
        return eventsHolder
    }
}