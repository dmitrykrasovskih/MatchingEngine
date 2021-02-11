package com.lykke.utils.alivestatus.processor

import com.lykke.utils.alivestatus.config.AliveStatusConfig
import com.lykke.utils.alivestatus.database.azure.AzureAliveStatusDatabaseAccessor

internal class AzureAliveStatusProcessorCreator(
        private val connectionString: String,
        private val tableName: String,
        private val appName: String,
        private val config: AliveStatusConfig
) : AliveStatusProcessorCreator {

    override fun createProcessor(): AliveStatusProcessor {
        return AliveStatusProcessor(
                AzureAliveStatusDatabaseAccessor(connectionString, tableName, appName, config.lifeTime),
                config
        )
    }
}