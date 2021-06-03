package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.CashSwapContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.CashSwapParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashSwapContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.wrappers.CashSwapOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashSwapOperationInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class CashSwapPreprocessor(
    contextParser: CashSwapContextParser,
    preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    private val cashOperationIdDatabaseAccessor: CashOperationIdDatabaseAccessor,
    private val cashSwapPreprocessorPersistenceManager: PersistenceManager,
    private val processedMessagesCache: ProcessedMessagesCache,
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
    @Qualifier("cashSwapPreProcessingLogger")
    private val logger: ThrottlingLogger
) :
    AbstractMessagePreprocessor<CashSwapParsedData, CashSwapOperationMessageWrapper>(
        contextParser,
        messageProcessingStatusHolder,
        preProcessedMessageQueue,
        logger
    ) {

    companion object {
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    @Autowired
    private lateinit var cashSwapOperationInputValidator: CashSwapOperationInputValidator

    override fun preProcessParsedData(parsedData: CashSwapParsedData): Boolean {
        val parsedMessageWrapper = parsedData.messageWrapper as CashSwapOperationMessageWrapper
        val context = parsedMessageWrapper.context as CashSwapContext
        if (messageProcessingStatusHolder.isCashSwapDisabled(
                context.swapOperation.asset1,
                context.swapOperation.asset2
            )
        ) {
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
            METRICS_LOGGER.logError(errorMessage)
            return false
        }

        return true
    }

    fun validateData(cashSwapParsedData: CashSwapParsedData): Boolean {
        try {
            cashSwapOperationInputValidator.performValidation(cashSwapParsedData)
        } catch (e: ValidationException) {
            processInvalidData(cashSwapParsedData, e.validationType, e.message)
            return false
        }

        return true
    }

    private fun processInvalidData(
        cashSwapParsedData: CashSwapParsedData,
        validationType: ValidationException.Validation,
        message: String
    ) {
        val messageWrapper = cashSwapParsedData.messageWrapper as CashSwapOperationMessageWrapper
        val context = messageWrapper.context as CashSwapContext
        logger.info("Input validation failed messageId: ${context.messageId}, details: $message")

        val persistSuccess =
            cashSwapPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))
        if (!persistSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeErrorResponse(messageWrapper, context, MessageStatusUtils.toMessageStatus(validationType), message)
        } catch (e: Exception) {
            logger.error("Error occurred during processing of invalid cash swap data, context $context", e)
            METRICS_LOGGER.logError("Error occurred during invalid data processing, ${messageWrapper.type} ${context.messageId}")
        }
    }

    private fun isMessageDuplicated(cashSwapParsedData: CashSwapParsedData): Boolean {
        val parsedMessageWrapper = cashSwapParsedData.messageWrapper as CashSwapOperationMessageWrapper
        val context = parsedMessageWrapper.context!!
        return cashOperationIdDatabaseAccessor.isAlreadyProcessed(
            parsedMessageWrapper.type.toString(),
            context.messageId
        )
    }

    private fun writeErrorResponse(
        messageWrapper: CashSwapOperationMessageWrapper,
        context: CashSwapContext,
        status: MessageStatus,
        errorMessage: String = StringUtils.EMPTY
    ) {
        messageWrapper.writeResponse(context.swapOperation.matchingEngineOperationId, status, errorMessage)
        logger.info(
            "Cash swap operation (${context.swapOperation.externalId}) " +
                    "from client ${context.swapOperation.walletId1}, " +
                    "asset ${context.swapOperation.asset1}, " +
                    "volume: ${NumberUtils.roundForPrint(context.swapOperation.volume1)}" +
                    "to client ${context.swapOperation.walletId2}, " +
                    "asset ${context.swapOperation.asset2}, " +
                    "volume: ${NumberUtils.roundForPrint(context.swapOperation.volume2)}" +
                    " : $errorMessage"
        )
    }

    override fun writeResponse(
        messageWrapper: CashSwapOperationMessageWrapper,
        status: MessageStatus,
        message: String?
    ) {
        val context = messageWrapper.context!!
        messageWrapper.writeResponse(context.swapOperation.matchingEngineOperationId, status, message)
        logger.info(
            "Cash swap operation (${context.swapOperation.externalId}) " +
                    "from client ${context.swapOperation.walletId1}, " +
                    "asset ${context.swapOperation.asset1}, " +
                    "volume: ${NumberUtils.roundForPrint(context.swapOperation.volume1)}" +
                    "to client ${context.swapOperation.walletId2}, " +
                    "asset ${context.swapOperation.asset2}, " +
                    "volume: ${NumberUtils.roundForPrint(context.swapOperation.volume2)}" +
                    " : $message"
        )
    }
}