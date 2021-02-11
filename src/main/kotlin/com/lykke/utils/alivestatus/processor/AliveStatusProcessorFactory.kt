package com.lykke.utils.alivestatus.processor

import com.lykke.utils.alivestatus.config.AliveStatusConfig

object AliveStatusProcessorFactory {

    private const val DEFAULT_AZURE_TABLE_NAME = "AliveStatus"

    fun createAzureProcessor(
            connectionString: String,
            tableName: String = DEFAULT_AZURE_TABLE_NAME,
            appName: String,
            config: AliveStatusConfig
    ) = AzureAliveStatusProcessorCreator(connectionString, tableName, appName, config).createProcessor()
}