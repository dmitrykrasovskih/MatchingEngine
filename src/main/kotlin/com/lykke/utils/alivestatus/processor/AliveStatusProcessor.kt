package com.lykke.utils.alivestatus.processor

import com.lykke.utils.AppInitializer
import com.lykke.utils.alivestatus.config.AliveStatusConfig
import com.lykke.utils.alivestatus.database.AliveStatusDatabaseAccessor
import org.apache.logging.log4j.LogManager
import kotlin.concurrent.fixedRateTimer

class AliveStatusProcessor internal constructor(
    private val dbAccessor: AliveStatusDatabaseAccessor,
    private val config: AliveStatusConfig
) : Runnable {

    companion object {
        private val LOGGER = LogManager.getLogger(AliveStatusProcessor::class.java.name)
    }

    private var isAlive = false

    override fun run() {
        dbAccessor.checkAndLock()
        isAlive = true

        fixedRateTimer(name = "AliveStatusUpdater", initialDelay = config.updatePeriod, period = config.updatePeriod) {
            try {
                keepAlive()
            } catch (e: Exception) {
                val errorMessage = "Unable to save a keep alive status: ${e.message}"
                AppInitializer.teeLog(errorMessage)
                LOGGER.error(errorMessage, e)
            }
        }

        Runtime.getRuntime().addShutdownHook(AliveStatusShutdownHook(this))
    }

    @Synchronized
    private fun keepAlive() {
        synchronized(this) {
            if (!isAlive) {
                return
            }
            dbAccessor.keepAlive()
        }
    }

    @Synchronized
    internal fun unlock() {
        synchronized(this) {
            dbAccessor.unlock()
            isAlive = false
        }
    }
}

