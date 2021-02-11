package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper

class CashInOutParsedData(
    messageWrapper: CashInOutOperationMessageWrapper,
    val assetId: String
) : ParsedData(messageWrapper)