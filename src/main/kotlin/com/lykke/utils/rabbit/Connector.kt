package com.lykke.utils.rabbit

import com.rabbitmq.client.Channel

interface Connector {
    fun createChannel(config: RabbitMqConfig): Channel
}