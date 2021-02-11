package com.lykke.utils.rabbit

data class RabbitMqConfig(
        val host: String? = null,
        val port: Int? = null,
        val username: String? = null,
        val password: String? = null,
        val exchange: String,
        val queue: String,
        val connectionTryInterval: Long?,
        val uri: String? = null
)