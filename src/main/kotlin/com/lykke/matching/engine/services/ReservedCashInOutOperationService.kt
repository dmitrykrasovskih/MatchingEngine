package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.context.ReservedCashInOutContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageSequenceNumberHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.LOW_BALANCE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.business.ReservedCashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*

@Service
class ReservedCashInOutOperationService @Autowired constructor(
    private val balancesHolder: BalancesHolder,
    private val reservedCashInOutOperationBusinessValidator: ReservedCashInOutOperationBusinessValidator,
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
    private val messageSender: MessageSender
) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val now = Date()
        val messageWrapper = genericMessageWrapper as ReservedCashInOutOperationMessageWrapper
        val reservedCashInOutContext = messageWrapper.context as ReservedCashInOutContext
        val reservedCashInOutOperation = reservedCashInOutContext.reservedCashInOutOperation
        val asset = reservedCashInOutOperation.asset!!

        LOGGER.debug(
            "Processing reserved cash in/out messageId: ${messageWrapper.messageId} " +
                    "operation (${reservedCashInOutOperation.externalId}) for client ${reservedCashInOutOperation.walletId}, " +
                    "asset ${asset.symbol}, reserved amount: ${reservedCashInOutOperation.reservedAmount}, swap amount: ${reservedCashInOutOperation.reservedSwapAmount}"
        )

        val matchingEngineOperationId = UUID.randomUUID().toString()
        val walletOperation = WalletOperation(
            reservedCashInOutOperation.brokerId,
            reservedCashInOutOperation.accountId,
            reservedCashInOutOperation.walletId,
            asset.symbol,
            BigDecimal.ZERO,
            reservedCashInOutOperation.reservedAmount,
            reservedCashInOutOperation.reservedSwapAmount
        )

        try {
            reservedCashInOutOperationBusinessValidator.performValidation(reservedCashInOutContext)
        } catch (e: ValidationException) {
            messageWrapper.writeResponse(
                matchingEngineOperationId,
                MessageStatusUtils.toMessageStatus(e.validationType),
                e.message
            )
            LOGGER.info(
                "Reserved cash in/out operation (${reservedCashInOutOperation.externalId}) for client ${reservedCashInOutOperation.walletId}" +
                        " asset ${asset.symbol}, volume: ${reservedCashInOutOperation.reservedAmount}: ${e.message}"
            )
            return
        }

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(listOf(walletOperation))
        } catch (e: BalanceException) {
            messageWrapper.writeResponse(matchingEngineOperationId, LOW_BALANCE, e.message)
            LOGGER.info("Reserved cash in/out operation (${reservedCashInOutOperation.externalId}) failed due to invalid balance: ${e.message}")
            return
        }

        val sequenceNumber = messageSequenceNumberHolder.getNewValue()

        val outgoingMessage = EventFactory.createReservedCashInOutEvent(
            sequenceNumber,
            reservedCashInOutContext.messageId,
            reservedCashInOutOperation.externalId!!,
            now,
            MessageType.CASH_IN_OUT_OPERATION,
            walletProcessor.getClientBalanceUpdates(),
            walletOperation
        )

        messageWrapper.processedMessage?.status = MessageStatus.OK
        messageWrapper.processedMessage?.matchingEngineId = reservedCashInOutOperation.matchingEngineOperationId

        val updated = walletProcessor.persistBalances(
            messageWrapper.processedMessage,
            null,
            null,
            sequenceNumber,
            outgoingMessage
        )
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            LOGGER.info(
                "Reserved cash in/out operation (${reservedCashInOutOperation.externalId}) for client ${reservedCashInOutOperation.walletId} " +
                        "asset ${asset.symbol}, volume: ${reservedCashInOutOperation.reservedAmount}: unable to save balance"
            )
            messageWrapper.writeResponse(matchingEngineOperationId, RUNTIME)
            return
        }
        walletProcessor.apply()

        messageSender.sendMessage(outgoingMessage)
        messageWrapper.writeResponse(reservedCashInOutOperation.matchingEngineOperationId, MessageStatus.OK, null)

        LOGGER.info(
            "Reserved cash in/out operation (${reservedCashInOutOperation.externalId}) for client ${reservedCashInOutOperation.walletId}, " +
                    "asset ${asset.symbol}, amount: ${reservedCashInOutOperation.reservedAmount} processed"
        )
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as ReservedCashInOutOperationMessageWrapper
        messageWrapper.writeResponse(messageWrapper.id, status)
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, processedMessage: ProcessedMessage) {
        val messageWrapper = genericMessageWrapper as ReservedCashInOutOperationMessageWrapper
        messageWrapper.writeResponse(
            processedMessage.matchingEngineId ?: messageWrapper.messageId,
            processedMessage.status ?: MessageStatus.DUPLICATE,
            processedMessage.statusReason ?: StringUtils.EMPTY
        )
    }
}