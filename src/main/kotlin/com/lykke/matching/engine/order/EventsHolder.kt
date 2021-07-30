package com.lykke.matching.engine.order

import com.lykke.matching.engine.outgoing.messages.v2.events.Event

data class EventsHolder(
    val trustedClientsEvent: Event<*>?,
    val clientsEvent: Event<*>?,
)
