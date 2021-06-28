package com.lykke.matching.engine.messages.wrappers

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.messages.MessageType
import com.lykke.utils.logging.ThrottlingLogger

abstract class MessageWrapper(
    val type: MessageType,
    val id: String,
    val messageId: String
) {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(MessageWrapper::class.java.name)
    }

    var messagePreProcessorStartTimestamp: Long? = null
    var messagePreProcessorEndTimestamp: Long? = null
    var writeResponseTime: Long? = null

    var processedMessage: ProcessedMessage? = null
    val startTimestamp: Long = System.nanoTime()
    var triedToPersist: Boolean = false
    var persisted: Boolean = false
}
