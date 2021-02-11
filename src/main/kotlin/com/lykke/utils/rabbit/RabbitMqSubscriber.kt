package com.lykke.utils.rabbit

open class RabbitMqSubscriber(
        private val config: RabbitMqConfig,
        connector: Connector,
        private val consumerFactory: ConsumerFactory
) : RabbitMqClient(config, connector) {

    companion object {
        private val DEFAULT_CONNECTION_TRY_INTERVAL = 1000L
    }

    override fun startClient() {
        channel!!.basicConsume(config.queue, true, consumerFactory.newConsumer(channel!!))
    }

    override fun connectionTryInterval() = config.connectionTryInterval ?: DEFAULT_CONNECTION_TRY_INTERVAL

    override fun logSuffix() = ", queue: ${config.queue}"
}