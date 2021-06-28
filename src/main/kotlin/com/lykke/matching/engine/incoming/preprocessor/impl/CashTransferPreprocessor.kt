package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashTransferOperationInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class CashTransferPreprocessor(
    contextParser: CashTransferContextParser,
    preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    private val cashOperationIdDatabaseAccessor: CashOperationIdDatabaseAccessor,
    private val cashTransferPreprocessorPersistenceManager: PersistenceManager,
    private val processedMessagesCache: ProcessedMessagesCache,
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
    @Qualifier("cashTransferPreProcessingLogger")
    private val logger: ThrottlingLogger
) :
    AbstractMessagePreprocessor<CashTransferParsedData, CashTransferOperationMessageWrapper>(
        contextParser,
        messageProcessingStatusHolder,
        preProcessedMessageQueue,
        logger
    ) {

    @Autowired
    private lateinit var cashTransferOperationInputValidator: CashTransferOperationInputValidator

    override fun preProcessParsedData(parsedData: CashTransferParsedData): Boolean {
        val parsedMessageWrapper = parsedData.messageWrapper as CashTransferOperationMessageWrapper
        val context = parsedMessageWrapper.context as CashTransferContext
        if (messageProcessingStatusHolder.isCashTransferDisabled(context.transferOperation.asset)) {
            writeResponse(parsedMessageWrapper, MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return false
        }

        if (!validateData(parsedData)) {
            return false
        }

        if (isMessageDuplicated(parsedData)) {
            writeResponse(parsedMessageWrapper, DUPLICATE)
            val errorMessage = "Message already processed: ${parsedMessageWrapper.type}: ${context.messageId}"
            logger.info(errorMessage)
            return false
        }

        return true
    }

    fun validateData(cashTransferParsedData: CashTransferParsedData): Boolean {
        try {
            cashTransferOperationInputValidator.performValidation(cashTransferParsedData)
        } catch (e: ValidationException) {
            processInvalidData(cashTransferParsedData, e.validationType, e.message)
            return false
        }

        return true
    }

    private fun processInvalidData(
        cashTransferParsedData: CashTransferParsedData,
        validationType: ValidationException.Validation,
        message: String
    ) {
        val messageWrapper = cashTransferParsedData.messageWrapper as CashTransferOperationMessageWrapper
        val context = messageWrapper.context as CashTransferContext
        logger.info("Input validation failed messageId: ${context.messageId}, details: $message")

        val persistSuccess =
            cashTransferPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))
        if (!persistSuccess) {
            throw Exception("Persistence error")
        }

        @Suppress("DuplicatedCode")
        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeErrorResponse(messageWrapper, context, MessageStatusUtils.toMessageStatus(validationType), message)
        } catch (e: Exception) {
            logger.error("Error occurred during processing of invalid cash transfer data, context $context", e)
        }
    }

    private fun isMessageDuplicated(cashTransferParsedData: CashTransferParsedData): Boolean {
        val parsedMessageWrapper = cashTransferParsedData.messageWrapper as CashTransferOperationMessageWrapper
        val context = parsedMessageWrapper.context!!
        return cashOperationIdDatabaseAccessor.isAlreadyProcessed(
            parsedMessageWrapper.type.toString(),
            context.messageId
        )
    }

    private fun writeErrorResponse(
        messageWrapper: CashTransferOperationMessageWrapper,
        context: CashTransferContext,
        status: MessageStatus,
        errorMessage: String = StringUtils.EMPTY
    ) {
        @Suppress("DuplicatedCode")
        messageWrapper.writeResponse(context.transferOperation.matchingEngineOperationId, status, errorMessage)
        logger.info(
            "Cash transfer operation (${context.transferOperation.externalId}) from client ${context.transferOperation.fromClientId} " +
                    "to client ${context.transferOperation.toClientId}, asset ${context.transferOperation.asset}," +
                    " volume: ${NumberUtils.roundForPrint(context.transferOperation.volume)}: $errorMessage"
        )
    }

    override fun writeResponse(
        messageWrapper: CashTransferOperationMessageWrapper,
        status: MessageStatus,
        message: String?
    ) {
        val context = messageWrapper.context!!
        messageWrapper.writeResponse(context.transferOperation.matchingEngineOperationId, status, message)
        logger.info(
            "Cash transfer operation (${context.transferOperation.externalId}) from client ${context.transferOperation.fromClientId} " +
                    "to client ${context.transferOperation.toClientId}, asset ${context.transferOperation.asset}," +
                    " volume: ${NumberUtils.roundForPrint(context.transferOperation.volume)}: $message"
        )
    }
}