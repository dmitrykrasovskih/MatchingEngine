package com.lykke.utils.files.clean.config

data class LogFilesCleanerConfig(val enabled: Boolean,
                                 val directory: String?,
                                 val period: Long?,
                                 val connectionString: String?,
                                 val blobContainerName: String?,
                                 val uploadDaysThreshold: Int?,
                                 val archiveDaysThreshold: Int?)