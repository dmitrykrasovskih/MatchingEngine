package com.lykke.utils.logging

import com.lykke.utils.number.PrintUtils
import com.lykke.utils.number.RoundingUtils
import org.apache.logging.log4j.Logger

class PerformanceLogger {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(PerformanceLogger::class.java.name)
    }

    private val logger: Logger?
    private val throttlingLogger: ThrottlingLogger?
    private val logCount: Long
    private val logMsgPrefix: String

    private constructor(logger: Logger?, throttlingLogger: ThrottlingLogger?, logCount: Long, logMsgPrefix: String) {
        this.logger = logger
        this.throttlingLogger = throttlingLogger
        this.logCount = logCount
        this.logMsgPrefix = logMsgPrefix
    }

    constructor(logger: Logger, logCount: Long, logMsgPrefix: String = "") : this(logger, null, logCount, logMsgPrefix)

    constructor(throttlingLogger: ThrottlingLogger, logCount: Long, logMsgPrefix: String = "") : this(
        null,
        throttlingLogger,
        logCount,
        logMsgPrefix
    )

    private var startTime: Long? = null
    private var startPersistTime: Long? = null
    private var endTime: Long? = null
    private var endPersistTime: Long? = null
    private var count: Long = 0
    private var totalTime: Double = 0.0
    private var totalPersistTime: Double = 0.0

    fun start() {
        startTime = System.nanoTime()
    }

    fun startPersist() {
        startPersistTime = System.nanoTime()
    }

    fun end() {
        endTime = System.nanoTime()
    }

    fun endPersist() {
        endPersistTime = System.nanoTime()
    }

    fun fixTime() {
        try {
            count++
            totalPersistTime += (endPersistTime!! - startPersistTime!!).toDouble() / logCount
            totalTime += (endTime!! - startTime!!).toDouble() / logCount

            if (count % logCount == 0L) {
                val prefix = if (logMsgPrefix.isNotEmpty()) "$logMsgPrefix$logCount. " else ""
                log(
                    prefix + "Total: ${PrintUtils.convertToString(totalTime)}. " +
                            " Persist: ${PrintUtils.convertToString(totalPersistTime)}, ${
                                RoundingUtils.roundForPrint2(
                                    100 * totalPersistTime / totalTime
                                )
                            } %"
                )
                totalPersistTime = 0.0
                totalTime = 0.0
            }
        } catch (e: Exception) {
            error(
                "Unable to fix time (count: $count, startTime: $startTime, endTime: $endTime, startPersistTime: $startPersistTime, endPersistTime: $endPersistTime, logCount: $logCount)",
                e
            )
        }
        startTime = null
        endTime = null
        startPersistTime = null
        endPersistTime = null
    }

    private fun log(message: String) {
        try {
            logger?.info(message)
            throttlingLogger?.info(message)
        } catch (e: Exception) {
            LOGGER.error("Unable to log message", e)
        }
    }

    private fun error(message: String, e: Throwable) {
        try {
            logger?.error(message, e)
            throttlingLogger?.error(message, e)
        } catch (e: Exception) {
            LOGGER.error("Unable to log error", e)
        }
    }
}