package com.lykke.utils.keepalive.http

import java.util.Date
import kotlin.concurrent.fixedRateTimer

class KeepAliveStarter {
    companion object {
        fun start(config: KeepAliveConfig, responseGetter: IsAliveResponseGetter = DefaultIsAliveResponseGetter(), appVersion: String? = null) {
            if (config.passive) {
                IsAliveServer(config.port!!, responseGetter).start()
                return
            }
            val keepAliveAccessor = HttpKeepAliveAccessor(config.path!!, config.name, appVersion!!)
            fixedRateTimer(name = "StatusUpdater", initialDelay = 0, period = config.interval!!) {
                keepAliveAccessor.updateKeepAlive(Date(), null)
            }
        }
    }
}