package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.daos.fee.v2.Fee
import com.lykke.matching.engine.exception.PersistenceException
import com.lykke.matching.engine.fee.FeeException
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.messages.v2.events.CashTransferEvent
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*

@Service
class CashTransferOperationService(
    private val balancesHolder: BalancesHolder,
    private val feeProcessor: FeeProcessor,
    private val cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator,
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
    private val messageSender: MessageSender
) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(CashTransferOperationService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val now = Date()
        val messageWrapper = genericMessageWrapper as CashTransferOperationMessageWrapper
        val cashTransferContext = messageWrapper.context!!
        val transferOperation = cashTransferContext.transferOperation

        val asset = transferOperation.asset
        LOGGER.debug(
            "Processing cash transfer operation ${transferOperation.externalId}) messageId: ${cashTransferContext.messageId}" +
                    " from client ${transferOperation.fromClientId} to client ${transferOperation.toClientId}, " +
                    "asset $asset, volume: ${NumberUtils.roundForPrint(transferOperation.volume)}, " +
                    "feeInstructions: ${transferOperation.fees}"
        )

        try {
            cashTransferOperationBusinessValidator.performValidation(cashTransferContext)
        } catch (e: ValidationException) {
            messageWrapper.writeResponse(
                transferOperation.matchingEngineOperationId,
                MessageStatusUtils.toMessageStatus(e.validationType),
                e.message
            )
            LOGGER.info(
                "Cash transfer operation (${cashTransferContext.transferOperation.externalId}) from client ${cashTransferContext.transferOperation.fromClientId} " +
                        "to client ${cashTransferContext.transferOperation.toClientId}, asset ${cashTransferContext.transferOperation.asset}," +
                        " volume: ${NumberUtils.roundForPrint(cashTransferContext.transferOperation.volume)}: ${e.message}"
            )
            return
        }

        val result = try {
            processTransferOperation(transferOperation, messageWrapper, cashTransferContext, now)
        } catch (e: FeeException) {
            messageWrapper.writeResponse(
                transferOperation.matchingEngineOperationId,
                INVALID_FEE,
                e.message
            )
            return
        } catch (e: BalanceException) {
            messageWrapper.writeResponse(
                transferOperation.matchingEngineOperationId,
                LOW_BALANCE,
                e.message
            )
            return
        } catch (e: PersistenceException) {
            messageWrapper.writeResponse(transferOperation.matchingEngineOperationId, RUNTIME, e.message)
            return
        }

        messageSender.sendMessage(result.outgoingMessage)

        messageWrapper.writeResponse(transferOperation.matchingEngineOperationId, OK, null)
        LOGGER.info(
            "Cash transfer operation (${transferOperation.externalId}) from client ${transferOperation.fromClientId} to client ${transferOperation.toClientId}," +
                    " asset $asset, volume: ${NumberUtils.roundForPrint(transferOperation.volume)} processed"
        )
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as CashTransferOperationMessageWrapper
        messageWrapper.writeResponse(messageWrapper.id, status)
    }

    private fun processTransferOperation(
        operation: TransferOperation,
        messageWrapper: MessageWrapper,
        cashTransferContext: CashTransferContext,
        date: Date
    ): OperationResult {
        val operations = LinkedList<WalletOperation>()

        val assetId = operation.asset!!.symbol
        operations.add(
            WalletOperation(
                operation.brokerId,
                operation.accountId,
                operation.fromClientId,
                assetId,
                -operation.volume
            )
        )
        val receiptOperation =
            WalletOperation(operation.brokerId, operation.accountId, operation.toClientId, assetId, operation.volume)
        operations.add(receiptOperation)

        val fees =
            feeProcessor.processFee(
                operation.brokerId,
                operation.accountId,
                operation.fees,
                receiptOperation,
                operations,
                balancesGetter = balancesHolder
            )

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
            .preProcess(operations, true)

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(cashTransferContext.processedMessage, null, null, sequenceNumber)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            throw PersistenceException("Unable to save balance")
        }
        val messageId = cashTransferContext.messageId
        walletProcessor.apply()

        val outgoingMessage = EventFactory.createCashTransferEvent(
            sequenceNumber,
            messageId,
            operation.externalId,
            date,
            MessageType.CASH_TRANSFER_OPERATION,
            walletProcessor.getClientBalanceUpdates(),
            operation,
            fees
        )

        return OperationResult(outgoingMessage, fees)
    }
}

private class OperationResult(
    val outgoingMessage: CashTransferEvent,
    val fees: List<Fee>
)