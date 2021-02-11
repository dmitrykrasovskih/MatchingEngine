package com.lykke.utils.alivestatus.daos.azure

import com.lykke.utils.alivestatus.database.azure.AzureAliveStatusDatabaseAccessor
import com.microsoft.azure.storage.table.TableServiceEntity
import java.util.Date

internal class AzureAliveStatus : TableServiceEntity {

    var running: Boolean = false
    var startTime: Date? = null
    var lastAliveTime: Date? = null
    var ip: String? = null

    constructor()

    constructor(appName: String, startTime: Date, ip: String, running: Boolean) : super(AzureAliveStatusDatabaseAccessor.PARTITION_KEY, appName) {
        this.startTime = startTime
        this.lastAliveTime = startTime
        this.ip = ip
        this.running = running
    }

    private fun appName(): String = rowKey

    override fun toString(): String {
        return "AzureAliveStatus{" +
                "appName=" + appName() +
                ", running=" + running +
                ", startTime=" + startTime +
                ", lastAliveTime=" + lastAliveTime +
                ", ip='" + ip + '\'' +
                '}'
    }
}