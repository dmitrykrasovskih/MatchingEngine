package com.lykke.matching.engine.incoming.parsers

import com.lykke.matching.engine.incoming.parsers.data.ParsedData
import com.lykke.matching.engine.messages.wrappers.MessageWrapper

interface ContextParser<out R: ParsedData, in T: MessageWrapper> {
    fun parse(messageWrapper: T): R
}