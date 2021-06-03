package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.messages.wrappers.MessageWrapper

class CashSwapParsedData(
    messageWrapper: MessageWrapper,
    val assetId1: String,
    val assetId2: String
) : ParsedData(messageWrapper)