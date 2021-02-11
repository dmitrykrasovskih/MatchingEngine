package com.lykke.utils.keepalive

import java.util.Date

internal interface KeepAliveAccessor {
    fun updateKeepAlive(date: Date, note: String?)
}