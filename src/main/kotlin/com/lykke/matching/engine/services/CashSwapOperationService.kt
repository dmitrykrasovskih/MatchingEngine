package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.CashSwapContext
import com.lykke.matching.engine.exception.PersistenceException
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.CashSwapOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.messages.v2.events.CashSwapEvent
import com.lykke.matching.engine.services.validators.business.CashSwapOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*

@Service
class CashSwapOperationService(
    private val balancesHolder: BalancesHolder,
    private val cashSwapOperationBusinessValidator: CashSwapOperationBusinessValidator,
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
    private val messageSender: MessageSender
) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val now = Date()
        val messageWrapper = genericMessageWrapper as CashSwapOperationMessageWrapper
        val cashSwapContext = messageWrapper.context!!
        val swapOperation = cashSwapContext.swapOperation

        LOGGER.debug(
            "Processing cash swap operation ${swapOperation.externalId}) messageId: ${cashSwapContext.messageId}" +
                    "from client ${swapOperation.walletId1}, " +
                    "asset ${swapOperation.asset1}, " +
                    "volume: ${NumberUtils.roundForPrint(swapOperation.volume1)}" +
                    "to client ${swapOperation.walletId2}, " +
                    "asset ${swapOperation.asset2}, " +
                    "volume: ${NumberUtils.roundForPrint(swapOperation.volume2)}"
        )

        try {
            cashSwapOperationBusinessValidator.performValidation(cashSwapContext)
        } catch (e: ValidationException) {
            messageWrapper.writeResponse(
                swapOperation.matchingEngineOperationId,
                MessageStatusUtils.toMessageStatus(e.validationType),
                e.message
            )
            LOGGER.info(
                "Cash swap operation ${swapOperation.externalId}) messageId: ${cashSwapContext.messageId}" +
                        "from client ${swapOperation.walletId1}, " +
                        "asset ${swapOperation.asset1}, " +
                        "volume: ${NumberUtils.roundForPrint(swapOperation.volume1)}" +
                        "to client ${swapOperation.walletId2}, " +
                        "asset ${swapOperation.asset2}, " +
                        "volume: ${NumberUtils.roundForPrint(swapOperation.volume2)}: ${e.message}"
            )
            return
        }

        val result = try {
            processSwapOperation(swapOperation, messageWrapper, cashSwapContext, now)
        } catch (e: BalanceException) {
            messageWrapper.writeResponse(
                swapOperation.matchingEngineOperationId,
                LOW_BALANCE,
                e.message
            )
            return
        } catch (e: PersistenceException) {
            messageWrapper.writeResponse(swapOperation.matchingEngineOperationId, RUNTIME, e.message)
            return
        }

        messageSender.sendMessage(result)

        messageWrapper.writeResponse(swapOperation.matchingEngineOperationId, OK, null)
        LOGGER.info(
            "Cash swap operation ${swapOperation.externalId}) messageId: ${cashSwapContext.messageId}" +
                    "from client ${swapOperation.walletId1}, " +
                    "asset ${swapOperation.asset1}, " +
                    "volume: ${NumberUtils.roundForPrint(swapOperation.volume1)}" +
                    "to client ${swapOperation.walletId2}, " +
                    "asset ${swapOperation.asset2}, " +
                    "volume: ${NumberUtils.roundForPrint(swapOperation.volume2)} processed"
        )
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as CashSwapOperationMessageWrapper
        messageWrapper.writeResponse(messageWrapper.id, status)
    }

    private fun processSwapOperation(
        operation: SwapOperation,
        messageWrapper: MessageWrapper,
        cashSwapContext: CashSwapContext,
        date: Date
    ): CashSwapEvent {
        val operations = LinkedList<WalletOperation>()

        val assetId1 = operation.asset1!!.symbol

        //take asset1 from wallet1 from reserved for swap
        operations.add(
            WalletOperation(
                operation.brokerId,
                operation.accountId1,
                operation.walletId1,
                assetId1,
                -operation.volume1,
                BigDecimal.ZERO,
                -operation.volume1
            )
        )

        //add asset1 to wallet2
        operations.add(
            WalletOperation(
                operation.brokerId,
                operation.accountId2,
                operation.walletId2,
                assetId1,
                operation.volume1
            )
        )

        val assetId2 = operation.asset2!!.symbol

        //take asset2 from wallet2 from reserved for swap
        operations.add(
            WalletOperation(
                operation.brokerId,
                operation.accountId2,
                operation.walletId2,
                assetId2,
                -operation.volume2,
                BigDecimal.ZERO,
                -operation.volume2
            )
        )

        //add asset2 to wallet1
        operations.add(
            WalletOperation(
                operation.brokerId,
                operation.accountId1,
                operation.walletId1,
                assetId2,
                operation.volume2
            )
        )

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
            .preProcess(operations, true)

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()
        val updated = walletProcessor.persistBalances(cashSwapContext.processedMessage, null, null, sequenceNumber)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            throw PersistenceException("Unable to save balance")
        }
        val messageId = cashSwapContext.messageId
        walletProcessor.apply()

        val outgoingMessage = EventFactory.createCashSwapEvent(
            sequenceNumber,
            messageId,
            operation.externalId,
            date,
            MessageType.CASH_SWAP_OPERATION,
            walletProcessor.getClientBalanceUpdates(),
            operation
        )

        return outgoingMessage
    }
}