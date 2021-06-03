package com.lykke.matching.engine.daos.context

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.deduplication.ProcessedMessage

data class CashSwapContext(
    val messageId: String,
    val swapOperation: SwapOperation,
    val processedMessage: ProcessedMessage
)