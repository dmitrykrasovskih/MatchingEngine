package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.ReservedCashInOutOperation
import com.lykke.matching.engine.deduplication.ProcessedMessage

data class ReservedCashInOutContext(
    val messageId: String,
    val processedMessage: ProcessedMessage,
    val reservedCashInOutOperation: ReservedCashInOutOperation
)