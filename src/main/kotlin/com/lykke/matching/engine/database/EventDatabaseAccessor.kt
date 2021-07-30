package com.lykke.matching.engine.database

import com.lykke.matching.engine.outgoing.messages.v2.events.Event

interface EventDatabaseAccessor {
    fun save(event: Event<*>)
    fun loadEvents(startSequenceId: Long): List<Event<*>>
}