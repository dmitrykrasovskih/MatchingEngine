package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.ParsedData
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue

abstract class AbstractMessagePreprocessor<T : ParsedData, WrapperType : MessageWrapper>(
    private val contextParser: ContextParser<T, WrapperType>,
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
    private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    private val logger: ThrottlingLogger
) : MessagePreprocessor<WrapperType> {
    override fun preProcess(messageWrapper: WrapperType) {
        try {
            parseAndPreProcess(messageWrapper)
        } catch (e: Exception) {
            handlePreProcessingException(e, messageWrapper)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseAndPreProcess(messageWrapper: WrapperType) {
        messageWrapper.messagePreProcessorStartTimestamp = System.nanoTime()
        val parsedData = parse(messageWrapper)
        val parsedMessageWrapper = parsedData.messageWrapper as WrapperType
        val preProcessSuccess = when {
            !messageProcessingStatusHolder.isMessageProcessingEnabled() -> {
                writeResponse(
                    parsedMessageWrapper,
                    MessageStatus.MESSAGE_PROCESSING_DISABLED,
                    "Message processing is disabled"
                )
                false
            }
            !messageProcessingStatusHolder.isHealthStatusOk() -> {
                val errorMessage = "Message processing is disabled"
                writeResponse(parsedMessageWrapper, MessageStatus.RUNTIME, errorMessage)
                logger.error(errorMessage)
                false
            }
            else -> preProcessParsedData(parsedData)
        }
        parsedMessageWrapper.messagePreProcessorEndTimestamp = System.nanoTime()
        if (preProcessSuccess) {
            preProcessedMessageQueue.put(parsedMessageWrapper)
        }
    }

    protected abstract fun preProcessParsedData(parsedData: T): Boolean

    abstract fun writeResponse(messageWrapper: WrapperType, status: MessageStatus, message: String? = null)

    abstract fun writeResponse(messageWrapper: WrapperType, processedMessage: ProcessedMessage)

    private fun parse(messageWrapper: WrapperType): T {
        return contextParser.parse(messageWrapper)
    }

    private fun handlePreProcessingException(exception: Exception, message: WrapperType) {
        try {
            val errorMessage = "Got error during message preprocessing"
            logger.error("$errorMessage: ${exception.message}", exception)

            writeResponse(message, MessageStatus.RUNTIME, errorMessage)
        } catch (e: Exception) {
            val errorMessage = "Got error during message preprocessing failure handling"
            e.addSuppressed(exception)
            logger.error(errorMessage, e)
        }
    }
}