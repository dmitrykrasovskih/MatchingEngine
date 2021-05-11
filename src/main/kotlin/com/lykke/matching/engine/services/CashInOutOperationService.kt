package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.converters.CashInOutOperationConverter
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*

@Service
class CashInOutOperationService(
    private val balancesHolder: BalancesHolder,
    private val feeProcessor: FeeProcessor,
    private val cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator,
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
    private val messageSender: MessageSender
) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(CashInOutOperationService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val now = Date()
        val messageWrapper = genericMessageWrapper as CashInOutOperationMessageWrapper
        val cashInOutContext = messageWrapper.context as CashInOutContext
        val cashInOutOperation = cashInOutContext.cashInOutOperation
        val feeInstructions = cashInOutOperation.feeInstructions
        val walletOperation = CashInOutOperationConverter.fromCashInOutOperationToWalletOperation(cashInOutOperation)

        val asset = cashInOutOperation.asset!!
        LOGGER.debug(
            "Processing cash in/out messageId: ${cashInOutContext.messageId} operation (${cashInOutOperation.externalId})" +
                    " for client ${cashInOutContext.cashInOutOperation.clientId}, asset ${asset.symbol}," +
                    " amount: ${NumberUtils.roundForPrint(walletOperation.amount)}, feeInstructions: $feeInstructions"
        )

        val operations = mutableListOf(walletOperation)

        try {
            cashInOutOperationBusinessValidator.performValidation(cashInOutContext)
        } catch (e: ValidationException) {
            messageWrapper.writeResponse(
                cashInOutOperation.matchingEngineOperationId,
                MessageStatusUtils.toMessageStatus(e.validationType),
                e.message
            )
            LOGGER.info(
                "Cash in/out operation (${cashInOutContext.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${cashInOutContext.cashInOutOperation.clientId}, " +
                        "asset ${cashInOutContext.cashInOutOperation.asset!!.symbol}, amount: ${
                            NumberUtils.roundForPrint(
                                cashInOutContext.cashInOutOperation.amount
                            )
                        }: ${e.message}"
            )
            return
        }

        val fees = try {
            feeProcessor.processFee(
                walletOperation.brokerId,
                walletOperation.accountId,
                feeInstructions,
                walletOperation,
                operations,
                balancesGetter = balancesHolder
            )
        } catch (e: FeeException) {
            messageWrapper.writeResponse(cashInOutOperation.matchingEngineOperationId, INVALID_FEE, e.message)
            LOGGER.info(
                "Cash in/out operation (${cashInOutContext.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${cashInOutContext.cashInOutOperation.clientId}, " +
                        "asset ${cashInOutContext.cashInOutOperation.asset!!.symbol}, amount: ${
                            NumberUtils.roundForPrint(
                                cashInOutContext.cashInOutOperation.amount
                            )
                        }: ${e.message}"
            )
            return
        }

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(operations)
        } catch (e: BalanceException) {
            messageWrapper.writeResponse(cashInOutOperation.matchingEngineOperationId, LOW_BALANCE, e.message)
            LOGGER.info(
                "Cash in/out operation (${cashInOutContext.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${cashInOutContext.cashInOutOperation.clientId}, " +
                        "asset ${cashInOutContext.cashInOutOperation.asset!!.symbol}, amount: ${
                            NumberUtils.roundForPrint(
                                cashInOutContext.cashInOutOperation.amount
                            )
                        }: ${e.message}"
            )
            return
        }

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(cashInOutContext.processedMessage, null, null, sequenceNumber)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            messageWrapper.writeResponse(cashInOutOperation.matchingEngineOperationId, RUNTIME, "System error")
            LOGGER.info(
                "Cash in/out operation (${cashInOutContext.cashInOutOperation.externalId}), messageId: ${messageWrapper.messageId} for client ${cashInOutContext.cashInOutOperation.clientId}, " +
                        "asset ${cashInOutContext.cashInOutOperation.asset!!.symbol}, amount: ${
                            NumberUtils.roundForPrint(
                                cashInOutContext.cashInOutOperation.amount
                            )
                        }: Unable to save balance"
            )
            return
        }
        walletProcessor.apply()

        val outgoingMessage = EventFactory.createCashInOutEvent(
            walletOperation.amount,
            sequenceNumber,
            cashInOutContext.messageId,
            cashInOutOperation.externalId!!,
            now,
            MessageType.CASH_IN_OUT_OPERATION,
            walletProcessor.getClientBalanceUpdates(),
            walletOperation,
            fees
        )

        messageSender.sendMessage(outgoingMessage)
        messageWrapper.writeResponse(cashInOutOperation.matchingEngineOperationId, OK, null)

        LOGGER.info(
            "Cash in/out walletOperation (${cashInOutOperation.externalId}) for client ${cashInOutContext.cashInOutOperation.clientId}, " +
                    "asset ${cashInOutOperation.asset.symbol}, " +
                    "amount: ${NumberUtils.roundForPrint(walletOperation.amount)} processed"
        )
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as CashInOutOperationMessageWrapper
        messageWrapper.writeResponse(messageWrapper.id, status)
    }
}