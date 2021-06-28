package com.lykke.matching.engine.utils

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.DependsOn
import org.springframework.context.annotation.Profile
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import java.util.stream.Collectors

@Component
@DependsOn("dynamicOutgoingQueueConfig")
@Profile("default", "!local_config")
class QueueSizeLogger @Autowired constructor(
    private val queues: Map<String, BlockingQueue<*>?>,
    private val config: Config
) {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(QueueSizeLogger::class.java.name)

        const val ENTRY_FORMAT = "%s: %d; "
        const val ENTRY_SIZE_LIMIT_FORMAT = "%s queue is higher than limit"
        const val LOG_THREAD_NAME = "QueueSizeLogger"
    }

    @Suppress("SpringElInspection")
    @Scheduled(
        fixedRateString = "#{Config.matchingEngine.queueConfig.queueSizeLoggerInterval}",
        initialDelayString = "#{Config.matchingEngine.queueConfig.queueSizeLoggerInterval}"
    )
    private fun log() {
        try {
            Thread.currentThread().name = LOG_THREAD_NAME
            val queueNameToQueueSize = getQueueNameToQueueSize(queues)

            logQueueSizes(queueNameToQueueSize)
            checkQueueSizeLimits(queueNameToQueueSize)
        } catch (e: Exception) {
            val message = "Unable to check queue sizes"
            LOGGER.error(message, e)
        }
    }

    private fun logQueueSizes(nameToQueueSize: Map<String, Int>) {
        val logString = nameToQueueSize
            .entries
            .stream()
            .map { entry -> ENTRY_FORMAT.format(entry.key, entry.value) }
            .collect(Collectors.joining(""))

        LOGGER.info(logString)
    }

    private fun checkQueueSizeLimits(nameToQueueSize: Map<String, Int>) {
        nameToQueueSize
            .forEach { entry ->
                if (entry.value > config.matchingEngine.queueConfig.queueSizeLimit) {
                    val message = ENTRY_SIZE_LIMIT_FORMAT.format(entry.key)
                    LOGGER.warn(message)
                }
            }
    }

    private fun getQueueNameToQueueSize(nameToQueue: Map<String, BlockingQueue<*>?>): Map<String, Int> {
        return nameToQueue.filter { it.value != null }.mapValues { entry -> entry.value!!.size }
    }
}