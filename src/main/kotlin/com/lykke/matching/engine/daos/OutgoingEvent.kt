package com.lykke.matching.engine.daos

import com.lykke.matching.engine.outgoing.messages.v2.events.Event

data class OutgoingEvent(
    val sequenceId: Long,
    val event: Event<*>
)