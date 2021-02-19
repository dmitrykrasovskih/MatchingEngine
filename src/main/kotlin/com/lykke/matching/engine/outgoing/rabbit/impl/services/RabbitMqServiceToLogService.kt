package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import org.apache.logging.log4j.LogManager
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service("rabbitMqService")
@Profile("local")
class RabbitMqServiceToLogService(gson: Gson) : AbstractRabbitMQToLogService<Event<*>>(gson, LOGGER) {
    companion object {
        private val LOGGER = LogManager.getLogger(RabbitMqServiceToLogService::class.java)
    }
}