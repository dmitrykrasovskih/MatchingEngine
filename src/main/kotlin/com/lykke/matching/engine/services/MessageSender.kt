package com.lykke.matching.engine.services

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class MessageSender(
    private val clientsEventsQueue: BlockingQueue<Event<*>>,
    private val trustedClientsEventsQueue: BlockingQueue<Event<*>>
) {

    fun sendTrustedClientsMessage(message: Event<*>) {
        trustedClientsEventsQueue.put(message)
    }

    fun sendMessage(message: Event<*>) {
        clientsEventsQueue.put(message)
    }
}