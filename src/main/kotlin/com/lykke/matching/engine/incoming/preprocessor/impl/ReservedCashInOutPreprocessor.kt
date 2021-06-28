package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.daos.context.ReservedCashInOutContext
import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.ReservedCashInOutParsedData
import com.lykke.matching.engine.incoming.parsers.impl.ReservedCashInOutContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.DUPLICATE
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class ReservedCashInOutPreprocessor(
    reservedCashInOutContextParser: ReservedCashInOutContextParser,
    preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    private val cashOperationIdDatabaseAccessor: CashOperationIdDatabaseAccessor,
    private val cashInOutOperationPreprocessorPersistenceManager: PersistenceManager,
    private val processedMessagesCache: ProcessedMessagesCache,
    messageProcessingStatusHolder: MessageProcessingStatusHolder,
    @Qualifier("reservedCashInOutPreProcessingLogger")
    private val logger: ThrottlingLogger
) :
    AbstractMessagePreprocessor<ReservedCashInOutParsedData, ReservedCashInOutOperationMessageWrapper>(
        reservedCashInOutContextParser,
        messageProcessingStatusHolder,
        preProcessedMessageQueue,
        logger
    ) {

    @Autowired
    private lateinit var reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator

    override fun preProcessParsedData(parsedData: ReservedCashInOutParsedData): Boolean {
        val parsedMessageWrapper = parsedData.messageWrapper as ReservedCashInOutOperationMessageWrapper
        val context = parsedMessageWrapper.context as ReservedCashInOutContext

        @Suppress("DuplicatedCode")
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

    private fun validateData(cashInOutParsedData: ReservedCashInOutParsedData): Boolean {
        try {
            reservedCashInOutOperationValidator.performValidation(cashInOutParsedData)
        } catch (e: ValidationException) {
            processInvalidData(cashInOutParsedData, e.validationType, e.message)
            return false
        }

        return true
    }

    private fun processInvalidData(
        cashInOutParsedData: ReservedCashInOutParsedData,
        validationType: ValidationException.Validation,
        message: String
    ) {
        val messageWrapper = cashInOutParsedData.messageWrapper as ReservedCashInOutOperationMessageWrapper
        val context = messageWrapper.context as ReservedCashInOutContext
        logger.info("Input validation failed messageId: ${context.messageId}, details: $message")

        val persistSuccess =
            cashInOutOperationPreprocessorPersistenceManager.persist(PersistenceData(context.processedMessage))
        if (!persistSuccess) {
            throw Exception("Persistence error")
        }

        try {
            processedMessagesCache.addMessage(context.processedMessage)
            writeErrorResponse(
                messageWrapper,
                context.reservedCashInOutOperation.matchingEngineOperationId,
                MessageStatusUtils.toMessageStatus(validationType),
                message
            )
        } catch (e: Exception) {
            logger.error("Error occurred during processing of invalid cash in/out data, context $context", e)
        }
    }

    private fun isMessageDuplicated(cashInOutParsedData: ReservedCashInOutParsedData): Boolean {
        val parsedMessageWrapper = cashInOutParsedData.messageWrapper as ReservedCashInOutOperationMessageWrapper
        val context = parsedMessageWrapper.context!!
        return cashOperationIdDatabaseAccessor.isAlreadyProcessed(
            parsedMessageWrapper.type.toString(),
            context.messageId
        )
    }

    private fun writeErrorResponse(
        messageWrapper: ReservedCashInOutOperationMessageWrapper,
        operationId: String,
        status: MessageStatus,
        errorMessage: String = StringUtils.EMPTY
    ) {
        val context = messageWrapper.context as ReservedCashInOutContext
        messageWrapper.writeResponse(operationId, status, errorMessage)
        logger.info(
            "Cash in/out operation (${context.reservedCashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${context.reservedCashInOutOperation.walletId}, " +
                    "asset ${context.reservedCashInOutOperation.asset!!.symbol}, amount: ${
                        NumberUtils.roundForPrint(
                            context.reservedCashInOutOperation.reservedAmount
                        )
                    }: $errorMessage"
        )
    }

    override fun writeResponse(
        messageWrapper: ReservedCashInOutOperationMessageWrapper,
        status: MessageStatus,
        message: String?
    ) {
        val context = messageWrapper.context as ReservedCashInOutContext
        messageWrapper.writeResponse(null, status, message)
        logger.info(
            "Reserved Cash in/out operation (${context.reservedCashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${context.reservedCashInOutOperation.walletId}, " +
                    "asset ${context.reservedCashInOutOperation.asset!!.symbol}, amount: ${
                        NumberUtils.roundForPrint(
                            context.reservedCashInOutOperation.reservedAmount
                        )
                    }: $message"
        )
    }
}