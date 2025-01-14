package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.LimitOrderCancelMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.LimitOrderCancelOperationInputValidator
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderCancelOperationPreprocessor(
    limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData, LimitOrderCancelMessageWrapper>,
    val limitOrderCancelOperationValidator: LimitOrderCancelOperationInputValidator,
    messageProcessingStatusHolder: MessageProcessingStatusHolder,
    preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    val limitOrderCancelOperationPreprocessorPersistenceManager: PersistenceManager,
    val processedMessagesCache: ProcessedMessagesCache,
    @Qualifier("limitOrderCancelPreProcessingLogger")
    private val logger: ThrottlingLogger
) :
    AbstractMessagePreprocessor<LimitOrderCancelOperationParsedData, LimitOrderCancelMessageWrapper>(
        limitOrderCancelOperationContextParser,
        messageProcessingStatusHolder,
        preProcessedMessageQueue,
        logger
    ) {

    override fun preProcessParsedData(parsedData: LimitOrderCancelOperationParsedData): Boolean {
        return validateData(parsedData)
    }

    private fun processInvalidData(
        data: LimitOrderCancelOperationParsedData,
        validationType: ValidationException.Validation,
        message: String
    ) {
        val messageWrapper = data.messageWrapper as LimitOrderCancelMessageWrapper
        val context = messageWrapper.context
        logger.info("Input validation failed messageId: ${context!!.messageId}, details: $message")

        val persistenceSuccess =
            limitOrderCancelOperationPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))

        if (!persistenceSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeResponse(messageWrapper, MessageStatusUtils.toMessageStatus(validationType), message)
        } catch (e: Exception) {
            logger.error("Error occurred during processing of invalid limit order cancel data, context $context", e)
        }
    }

    private fun validateData(data: LimitOrderCancelOperationParsedData): Boolean {
        try {
            limitOrderCancelOperationValidator.performValidation(data)
        } catch (e: ValidationException) {
            processInvalidData(data, e.validationType, e.message)
            return false
        }
        return true
    }

    override fun writeResponse(
        messageWrapper: LimitOrderCancelMessageWrapper,
        status: MessageStatus,
        message: String?
    ) {
        messageWrapper.writeResponse(status, message)
    }

    override fun writeResponse(messageWrapper: LimitOrderCancelMessageWrapper, processedMessage: ProcessedMessage) {
        //do nothing
    }
}