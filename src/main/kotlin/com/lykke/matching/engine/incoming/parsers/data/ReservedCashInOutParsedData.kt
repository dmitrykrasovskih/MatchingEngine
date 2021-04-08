package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper

class ReservedCashInOutParsedData(
    messageWrapper: ReservedCashInOutOperationMessageWrapper,
    val assetId: String
) : ParsedData(messageWrapper)