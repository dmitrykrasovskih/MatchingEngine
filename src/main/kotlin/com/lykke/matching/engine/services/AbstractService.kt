package com.lykke.matching.engine.services

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.MessageWrapper

interface AbstractService {
    fun processMessage(genericMessageWrapper: MessageWrapper)
    fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus)
    fun writeResponse(genericMessageWrapper: MessageWrapper, processedMessage: ProcessedMessage)
}