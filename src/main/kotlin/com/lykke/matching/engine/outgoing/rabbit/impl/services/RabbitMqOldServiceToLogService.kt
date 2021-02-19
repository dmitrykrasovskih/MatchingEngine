package com.lykke.matching.engine.outgoing.rabbit.impl.services

import com.google.gson.Gson
import org.apache.logging.log4j.LogManager
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service

@Service("rabbitMqOldService")
@Profile("local-config")
@Deprecated("consider to use new message format")
class RabbitMqOldServiceToLogService(gson: Gson) : AbstractRabbitMQToLogService<Any>(gson, LOGGER) {
    companion object {
        private val LOGGER = LogManager.getLogger(RabbitMqOldServiceToLogService::class.java)
    }
}