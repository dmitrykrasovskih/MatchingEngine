package com.lykke.utils.rabbit

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import java.io.Closeable

abstract class RabbitMqClient(
        private val config: RabbitMqConfig,
        private val connector: Connector
) : Closeable, Thread() {

    companion object {
        private val LOGGER = Logger.getLogger(RabbitMqClient::class.java.name)
    }

    protected var channel: Channel? = null

    open protected fun logSuffix() = ""

    open protected fun connectionTryInterval() = config.connectionTryInterval

    abstract protected fun startClient()

    protected fun connect(): Boolean {
        val host = config.host
        val port = config.port
        val exchangeName = config.exchange
        val uri = getUriWithoutCredentials(config.uri) ?: "${config.host}:$port"

        val logSuffix = "$uri, exchange: $exchangeName${logSuffix()}"

        LOGGER.info("Connecting to RabbitMQ: $logSuffix")

        return try {
            channel = connector.createChannel(config)
            LOGGER.info("Connected to RabbitMQ: $logSuffix")
            true
        } catch (e: Exception) {
            LOGGER.error("Unable to connect to RabbitMQ: $logSuffix: ${e.message}", e)
            false
        }
    }

    override fun close() {
        this.channel?.connection?.close()
    }

    override fun run() {
        val connectionTryInterval = connectionTryInterval()
        while (!connect()) {
            if (connectionTryInterval == null) {
                continue
            }
            sleep(connectionTryInterval)
        }
        startClient()
    }


    private fun getUriWithoutCredentials(uri: String?): String? {
        if (StringUtils.isEmpty(uri)) {
            return null
        }
        val tmpFactory = ConnectionFactory()
        tmpFactory.setUri(uri)
        tmpFactory.host
        return "${tmpFactory.host}:${tmpFactory.port}"
    }
}