package com.lykke.utils.alivestatus.database.azure

import com.lykke.utils.alivestatus.daos.AliveStatus
import com.lykke.utils.alivestatus.daos.azure.AzureAliveStatus
import com.lykke.utils.alivestatus.database.AliveStatusDatabaseAccessor
import com.lykke.utils.alivestatus.exception.CheckAppInstanceRunningException
import com.lykke.utils.azure.getOrCreateTable
import com.microsoft.azure.storage.table.CloudTable
import com.microsoft.azure.storage.table.TableOperation
import org.apache.log4j.Logger
import java.net.InetAddress
import java.util.Date


internal class AzureAliveStatusDatabaseAccessor(private val connectionString: String, private val tableName: String, private val appName: String, private val lifeTime: Long) : AliveStatusDatabaseAccessor {
    companion object {
        private val LOGGER = Logger.getLogger(AzureAliveStatusDatabaseAccessor::class.java.name)
        internal const val PARTITION_KEY = "AliveStatus"
    }

    private val aliveStatusTable: CloudTable = getOrCreateTable(connectionString, tableName)
    private var aliveStatus: AzureAliveStatus? = null

    override fun checkAndLock() {
        try {
            val retrieveOperation = TableOperation.retrieve(PARTITION_KEY, appName, AzureAliveStatus::class.java)
            val currentAliveStatus = aliveStatusTable.execute(retrieveOperation).getResultAsType<AzureAliveStatus>()
            val now = Date()

            if (currentAliveStatus?.running == true && currentAliveStatus.lastAliveTime != null && currentAliveStatus.lastAliveTime!!.time > now.time - lifeTime) {
                throw CheckAppInstanceRunningException("Another app instance is already running", AliveStatus(currentAliveStatus.startTime!!, currentAliveStatus.lastAliveTime!!, currentAliveStatus.ip!!, currentAliveStatus.running))
            }

            val ip = InetAddress.getLocalHost().hostAddress
            val saveOperation = if (currentAliveStatus != null) {
                currentAliveStatus.ip = ip
                currentAliveStatus.startTime = now
                currentAliveStatus.running = true
                currentAliveStatus.lastAliveTime = now
                aliveStatus = currentAliveStatus
                TableOperation.merge(aliveStatus)
            } else {
                aliveStatus = AzureAliveStatus(appName, now, ip, true)
                TableOperation.insert(aliveStatus)
            }

            aliveStatusTable.execute(saveOperation)
        } catch (e: CheckAppInstanceRunningException) {
            throw e
        } catch (e: Exception) {
            val message = "Unable to check if app is running"
            LOGGER.error(message, e)
            throw CheckAppInstanceRunningException(message)
        }
    }

    override fun keepAlive() {
        try {
            aliveStatus!!.lastAliveTime = Date()
            aliveStatusTable.execute(TableOperation.merge(aliveStatus))
        } catch (e: CheckAppInstanceRunningException) {
            throw e
        } catch (e: Exception) {
            val message = "Unable to save last alive time"
            LOGGER.error(message, e)
            throw CheckAppInstanceRunningException(message)
        }
    }

    override fun unlock() {
        try {
            aliveStatus!!.running = false
            // reinitialize table for correct call from AliveStatusShutdownHook
            getOrCreateTable(connectionString, tableName).execute(TableOperation.merge(aliveStatus))
        } catch (e: CheckAppInstanceRunningException) {
            throw e
        } catch (e: Exception) {
            val message = "Unable to save no running flag"
            LOGGER.error(message, e)
            throw CheckAppInstanceRunningException(message)
        }
    }
}