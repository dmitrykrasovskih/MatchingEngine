package com.lykke.utils.alivestatus.database

internal interface AliveStatusDatabaseAccessor {
    fun checkAndLock()
    fun keepAlive()
    fun unlock()
}