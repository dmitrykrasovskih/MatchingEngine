package com.lykke.utils.logging

import com.lykke.utils.logging.config.SlackNotificationConfig
import com.lykke.utils.queue.azure.AzureQueueWriter
import org.apache.logging.log4j.LogManager
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.fixedRateTimer

class MetricsLogger private constructor() {

    companion object {
        private val LOGGER = LogManager.getLogger(MetricsLogger::class.java.name)
        private val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")
        private const val TYPE_ERROR = "Errors"
        private const val TYPE_WARNING = "Warnings"
        private lateinit var azureQueueConnectionString: String
        private lateinit var queueName: String
        private var sender: String = ""

        internal val ERROR_QUEUE = LinkedBlockingQueue<LoggableObject>()
        internal var throttlingLimit: Int = 0
        internal val sentTimestamps = ConcurrentHashMap<String, Long>()

        fun init(senderName: String, config: SlackNotificationConfig) {
            azureQueueConnectionString = config.azureQueue!!.connectionString
            queueName = config.azureQueue.queueName
            AzureQueueLogger(azureQueueConnectionString, queueName, ERROR_QUEUE).start()

            throttlingLimit = config.throttlingLimitSeconds * 1000
            sender = senderName

            fixedRateTimer(
                name = "MetricsLoggerCleaner",
                initialDelay = config.cleanerInterval,
                period = config.cleanerInterval
            ) {
                clearSentMessageTimestamps(config.messagesTtlMinutes)
            }
        }

        fun getLogger(): MetricsLogger = MetricsLogger()

        internal fun clearSentMessageTimestamps(ttlMinutes: Int) {
            var removedItems = 0
            val threshold = Date().time - ttlMinutes * 60 * 1000
            val iterator = sentTimestamps.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry.value < threshold) {
                    iterator.remove()
                    removedItems++
                }
            }
            LOGGER.debug("Removed $removedItems from MetricsLogger")
        }

        /** Saves 'Warnings' msg directly to azure queue without common thread using */
        fun logWarning(message: String) {
            log(TYPE_WARNING, message)
        }

        /** Saves 'Errors' msg directly to azure queue without common thread using */
        fun logError(message: String) {
            log(TYPE_ERROR, message)
        }

        private fun log(type: String, message: String) {
            val error = Error(type, sender, "${LocalDateTime.now().format(DATE_TIME_FORMATTER)}: $message")
            AzureQueueWriter(azureQueueConnectionString, queueName).write(error.getJson())
        }
    }

    fun logError(message: String, exception: Exception? = null) {
        log(TYPE_ERROR, message, exception)
    }

    fun logWarning(message: String, exception: Exception? = null) {
        log(TYPE_WARNING, message, exception)
    }

    private fun log(type: String, message: String, exception: Exception? = null) {
        if (messageWasSentWithinTimeout(type, message)) {
            return
        }
        ERROR_QUEUE.put(
            Error(
                type,
                sender,
                "${LocalDateTime.now().format(DATE_TIME_FORMATTER)}: $message ${exception?.message ?: ""}"
            )
        )
        sentTimestamps["$type-$message"] = Date().time
    }

    private fun messageWasSentWithinTimeout(type: String, message: String): Boolean {
        val lastSentTimestamp = sentTimestamps["$type-$message"]
        return lastSentTimestamp != null && lastSentTimestamp > Date().time - throttlingLimit
    }
}