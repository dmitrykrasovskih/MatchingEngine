package com.lykke.utils.rabbit

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Consumer

interface ConsumerFactory {
    fun newConsumer(channel: Channel): Consumer
}