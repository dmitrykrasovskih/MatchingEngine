package com.lykke.matching.engine.services

import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.SingleLimitOrderMessageWrapper
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.util.*

@Service
class SingleLimitOrderService(
    private val executionContextFactory: ExecutionContextFactory,
    private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
    private val stopOrderBookProcessor: StopOrderBookProcessor,
    private val executionDataApplyService: ExecutionDataApplyService,
    private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
    private val balancesHolder: BalancesHolder
) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(SingleLimitOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${SingleLimitOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0

    private var logCount = 100
    private var totalTime: Double = 0.0

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val messageWrapper = genericMessageWrapper as SingleLimitOrderMessageWrapper
        val context = messageWrapper.context
        val parsedMessage = messageWrapper.parsedMessage

        val now = Date()
        LOGGER.info("Got limit order: $context")

        val assetPair = context!!.assetPair!!

        val order = context.limitOrder
        order.register(now)

        val actualWalletVersion = balancesHolder.getBalanceVersion(
            order.clientId,
            if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
        )
        if (parsedMessage.walletVersion >= 0 && actualWalletVersion != parsedMessage.walletVersion) {
            LOGGER.error(
                "Invalid wallet version: ${parsedMessage.walletVersion}, actual: $actualWalletVersion"
            )
            messageWrapper.writeResponse(
                MessageStatus.INVALID_WALLET_VERSION,
                "Invalid wallet version",
                order.id,
                actualWalletVersion
            )
            return
        }

        val startTime = System.nanoTime()
        val executionContext = executionContextFactory.create(context.messageId,
            messageWrapper.id,
            MessageType.LIMIT_ORDER,
            messageWrapper.processedMessage,
            mapOf(Pair(context.assetPair!!.symbol, context.assetPair)),
            now,
            LOGGER,
            mapOf(
                Pair(context.baseAsset!!.symbol, context.baseAsset),
                Pair(context.quotingAsset!!.symbol, context.quotingAsset)
            ),
            context.validationResult?.let { mapOf(Pair(order.id, it)) } ?: emptyMap())

        previousLimitOrdersProcessor.cancelAndReplaceOrders(
            order.clientId,
            order.assetPairId,
            context.isCancelOrders,
            order.isBuySide(),
            !order.isBuySide(),
            emptyMap(),
            emptyMap(),
            executionContext
        )
        val processedOrder = genericLimitOrdersProcessor.processOrders(listOf(order), executionContext).single()
        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)
        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)

        if (!persisted) {
            val message = "Unable to save result data"
            LOGGER.error("$message (order external id: ${order.externalId})")
            messageWrapper.writeResponse(
                MessageStatus.RUNTIME,
                message,
                processedOrder.order.id
            )
            return
        }

        if (processedOrder.accepted) {
            messageWrapper.writeResponse(
                MessageStatus.OK, null, processedOrder.order.id,
                balancesHolder.getBalanceVersion(
                    order.clientId,
                    if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
                )
            )
        } else {
            messageWrapper.writeResponse(
                MessageStatusUtils.toMessageStatus(processedOrder.order.status),
                processedOrder.reason,
                processedOrder.order.id
            )
        }

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as SingleLimitOrderMessageWrapper
        messageWrapper.writeResponse(status)
    }
}

