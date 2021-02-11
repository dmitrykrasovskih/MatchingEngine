package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.messages.wrappers.MessageWrapper

class SingleLimitOrderParsedData(messageWrapper: MessageWrapper, val inputAssetPairId: String): ParsedData(messageWrapper)