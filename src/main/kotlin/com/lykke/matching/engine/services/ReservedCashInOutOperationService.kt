package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.LOW_BALANCE
import com.lykke.matching.engine.messages.MessageStatus.RUNTIME
import com.lykke.matching.engine.messages.MessageType.RESERVED_CASH_IN_OUT_OPERATION
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.lykke.matching.engine.outgoing.messages.ReservedCashOperation
import com.lykke.matching.engine.services.validators.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue

@Service
class ReservedCashInOutOperationService @Autowired constructor(
    private val assetsHolder: AssetsHolder,
    private val balancesHolder: BalancesHolder,
    private val reservedCashOperationQueue: BlockingQueue<ReservedCashOperation>,
    private val reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator,
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder
) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val messageWrapper = genericMessageWrapper as ReservedCashInOutOperationMessageWrapper
        val message = messageWrapper.parsedMessage
        val asset = assetsHolder.getAsset(message.assetId)

        LOGGER.debug(
            "Processing reserved cash in/out messageId: ${messageWrapper.messageId} " +
                    "operation (${message.id}) for client ${message.walletId}, " +
                    "asset ${message.assetId}, amount: ${message.reservedVolume}, swap amount: ${message.reservedForSwapVolume}"
        )

        val now = Date()
        val matchingEngineOperationId = UUID.randomUUID().toString()
        val operation = WalletOperation(
            message.brokerId,
            message.accountId,
            message.walletId,
            message.assetId,
            BigDecimal.ZERO,
            if (!message.reservedVolume.isNullOrEmpty()) BigDecimal(message.reservedVolume) else BigDecimal.ZERO,
            if (!message.reservedForSwapVolume.isNullOrEmpty()) BigDecimal(message.reservedForSwapVolume) else BigDecimal.ZERO
        )

        try {
            reservedCashInOutOperationValidator.performValidation(message)
        } catch (e: ValidationException) {
            messageWrapper.writeResponse(
                matchingEngineOperationId,
                MessageStatusUtils.toMessageStatus(e.validationType),
                e.message
            )
            LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.walletId} asset ${message.assetId}, volume: ${message.reservedVolume}: ${e.message}")
            return
        }

        val accuracy = asset.accuracy

        val walletProcessor = balancesHolder.createWalletProcessor(LOGGER)
        try {
            walletProcessor.preProcess(listOf(operation))
        } catch (e: BalanceException) {
            messageWrapper.writeResponse(matchingEngineOperationId, LOW_BALANCE, e.message)
            LOGGER.info("Reserved cash in/out operation (${message.id}) failed due to invalid balance: ${e.message}")
            return
        }

        val updated = walletProcessor.persistBalances(messageWrapper.processedMessage, null, null, null)
        messageWrapper.triedToPersist = true
        messageWrapper.persisted = updated
        if (!updated) {
            LOGGER.info("Reserved cash in/out operation (${message.id}) for client ${message.walletId} asset ${message.assetId}, volume: ${message.reservedVolume}: unable to save balance")
            messageWrapper.writeResponse(matchingEngineOperationId, RUNTIME)
            return
        }
        walletProcessor.apply()
            .sendNotification(message.id, RESERVED_CASH_IN_OUT_OPERATION.name, messageWrapper.messageId)

        reservedCashOperationQueue.put(
            ReservedCashOperation(
                message.id,
                operation.clientId,
                now,
                if (operation.reservedAmount != BigDecimal.ZERO) NumberUtils.setScaleRoundHalfUp(
                    operation.reservedAmount,
                    accuracy
                ).toPlainString() else "0.0",
                if (operation.reservedForSwapAmount != BigDecimal.ZERO) NumberUtils.setScaleRoundHalfUp(
                    operation.reservedForSwapAmount,
                    accuracy
                ).toPlainString() else "0.0",
                operation.assetId,
                messageWrapper.messageId
            )
        )

        messageWrapper.writeResponse(matchingEngineOperationId, MessageStatus.OK, null)

        LOGGER.info(
            "Reserved cash in/out operation (${message.id}) for client ${message.walletId}, " +
                    "asset ${message.assetId}, amount: ${message.reservedVolume} processed"
        )
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as ReservedCashInOutOperationMessageWrapper
        messageWrapper.writeResponse(messageWrapper.id, status)
    }

    private fun isCashIn(amount: Double): Boolean {
        return amount > 0
    }
}