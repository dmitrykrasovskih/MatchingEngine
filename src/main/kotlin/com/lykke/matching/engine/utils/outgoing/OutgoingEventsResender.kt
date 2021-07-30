package com.lykke.matching.engine.utils.outgoing

import com.lykke.matching.engine.database.EventDatabaseAccessor
import com.lykke.matching.engine.database.MessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.services.MessageSender
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(2)
class OutgoingEventsResender @Autowired constructor(
    private val eventDatabaseAccessor: EventDatabaseAccessor,
    private val redisSentMessageSequenceNumberDatabaseAccessor: MessageSequenceNumberDatabaseAccessor,
    private val messageSender: MessageSender
) : ApplicationRunner {

    override fun run(args: ApplicationArguments?) {
        resendEvents()
    }

    companion object {
        private val LOGGER = Logger.getLogger(OutgoingEventsResender::class.java.name)
    }

    fun resendEvents() {
        LOGGER.info("Starting events resender")
        val lastSentSequenceId = redisSentMessageSequenceNumberDatabaseAccessor.getSequenceNumber()
        LOGGER.info("Last sent sequence id: $lastSentSequenceId")
        val events = eventDatabaseAccessor.loadEvents(lastSentSequenceId)
        if (events.isNotEmpty()) {
            LOGGER.info("Total ${events.size} events to resend")

            events.sortedBy { event -> event.sequenceNumber() }.forEach { event ->
                messageSender.sendMessage(event)
            }
            LOGGER.info("Resent ${events.size} events from ${events.minOf { it.sequenceNumber() }} to ${events.maxOf { it.sequenceNumber() }}")
        } else {
            LOGGER.info("No events to resend")
        }
    }
}