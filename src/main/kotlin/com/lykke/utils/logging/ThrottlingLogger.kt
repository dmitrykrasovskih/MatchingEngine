package com.lykke.utils.logging

import com.lykke.utils.logging.config.ThrottlingLoggerConfig
import org.apache.log4j.Logger
import java.lang.Exception
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.concurrent.fixedRateTimer

class ThrottlingLogger private constructor(private val name: String) {

    companion object {
        private val LOGGER = Logger.getLogger(ThrottlingLogger::class.java.name)

        private const val DEFAULT_TTL_MINUTES = 60
        private val DEFAULT_CLEANER_INTERVAL = TimeUnit.HOURS.toMillis(3)

        private var throttlingLimit: Long = 0
        private var messagesTtlMinutes = DEFAULT_TTL_MINUTES
        private var cleanerInterval = DEFAULT_CLEANER_INTERVAL

        private val sentTimestamps = ConcurrentHashMap<String, Long>()

        fun init(config: ThrottlingLoggerConfig) {
            throttlingLimit = TimeUnit.SECONDS.toMillis(config.limitSeconds.toLong())
        }

        private fun clearSentMessageTimestamps(ttlMinutes: Int) {
            var removedItems = 0
            val threshold = Date().time - TimeUnit.MINUTES.toMillis(ttlMinutes.toLong())
            val iterator = sentTimestamps.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry.value < threshold) {
                    iterator.remove()
                    removedItems++
                }
            }
            LOGGER.debug("Removed $removedItems from ErrorsLogger")
        }

        fun getLogger(name: String): ThrottlingLogger = ThrottlingLogger(name)

        init {
            fixedRateTimer(name = "ThrottlingLoggerCleaner", initialDelay = cleanerInterval, period = cleanerInterval, daemon = true) {
                try {
                    clearSentMessageTimestamps(messagesTtlMinutes)
                } catch (e: Exception) {
                    LOGGER.error("Unable to clear sent message timestamps")
                }
            }
        }
    }

    private val logger: Logger = Logger.getLogger(name)

    fun debug(message: String) {
        logger.debug(message)
    }

    fun warn(message: String) {
        logger.warn(message)
    }

    fun info(message: String) {
        logger.info(message)
    }

    fun error(message: String) {
        logger.error(message)
    }

    fun error(message: String, t: Throwable) {
        if (errorWasLoggedWithinTimeout(message)) {
            return
        }
        logger.error(message, t)
        sentTimestamps[getKey(message)] = Date().time
    }

    private fun errorWasLoggedWithinTimeout(errorMessage: String): Boolean {
        val lastSentTimestamp = sentTimestamps[getKey(errorMessage)]
        return lastSentTimestamp != null && lastSentTimestamp > Date().time - throttlingLimit
    }

    private fun getKey(message: String): String {
        return "$name:$message"
    }

}