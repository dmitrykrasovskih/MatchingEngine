package com.lykke.utils.logging.config

data class AzureQueueConfig(
        val connectionString: String,
        val queueName: String
)