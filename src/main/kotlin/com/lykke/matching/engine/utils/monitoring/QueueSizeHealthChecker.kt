package com.lykke.matching.engine.utils.monitoring

import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.annotation.Scheduled
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

class QueueSizeHealthChecker(
    private val monitoredComponent: MonitoredComponent,
    private val nameToInputQueue: Map<String, BlockingQueue<*>>,
    private val queueMaxSize: Int,
    private val queueRecoverSize: Int
) {

    private companion object {
        val LOGGER = ThrottlingLogger.getLogger(QueueSizeHealthChecker::class.java.name)

        const val QUEUE_REACHED_THRESHOLD_MESSAGE =
            "Queue: %s, has reached max size threshold, current queue size is %d"
        const val QUEUE_RECOVERED_MESSAGE = "Queue: %s, has normal size again, current queue size is %d"
    }

    private var longQueues = HashSet<String>()
    private var lastCheckLongQueuesSize = 0

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @PostConstruct
    fun init() {
        val monitoredQueueNames = nameToInputQueue.keys.joinToString()
        LOGGER.info("Starting health monitoring for queues: $monitoredQueueNames")
    }

    @Suppress("SpringElInspection")
    @Scheduled(
        fixedRateString = "#{Config.matchingEngine.queueConfig.queueSizeHealthCheckInterval}",
        initialDelayString = "#{Config.matchingEngine.queueConfig.queueSizeHealthCheckInterval}"
    )
    fun checkQueueSize() {
        nameToInputQueue.forEach {
            checkQueueReachedMaxLimit(it)
            checkQueueRecovered(it)
        }

        sendHealthEventIfNeeded()
        lastCheckLongQueuesSize = longQueues.size
    }

    private fun sendHealthEventIfNeeded() {
        if (lastCheckLongQueuesSize == longQueues.size) {
            return
        }

        if (longQueues.isNotEmpty()) {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(false, monitoredComponent))
        } else {
            applicationEventPublisher.publishEvent(HealthMonitorEvent(true, monitoredComponent))
        }
    }

    private fun checkQueueRecovered(nameToQueue: Map.Entry<String, BlockingQueue<*>>) {
        if (nameToQueue.value.size <= queueRecoverSize && longQueues.remove(nameToQueue.key)) {
            val logMessage = QUEUE_RECOVERED_MESSAGE.format(nameToQueue.key, nameToQueue.value.size)
            LOGGER.info(logMessage)
        }
    }

    private fun checkQueueReachedMaxLimit(nameToQueue: Map.Entry<String, BlockingQueue<*>>) {
        if (nameToQueue.value.size >= queueMaxSize && !longQueues.contains(nameToQueue.key)) {
            longQueues.add(nameToQueue.key)
            val logMessage = QUEUE_REACHED_THRESHOLD_MESSAGE.format(nameToQueue.key, nameToQueue.value.size)
            LOGGER.error(logMessage)
        }
    }
}