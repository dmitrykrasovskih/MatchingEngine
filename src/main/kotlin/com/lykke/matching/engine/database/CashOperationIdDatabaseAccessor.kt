package com.lykke.matching.engine.database

import com.lykke.matching.engine.deduplication.ProcessedMessage

interface CashOperationIdDatabaseAccessor {
    fun getProcessedMessage(type: String, id: String): ProcessedMessage?
}