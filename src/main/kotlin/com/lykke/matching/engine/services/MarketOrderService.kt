package com.lykke.matching.engine.services

import com.lykke.matching.engine.balance.BalanceException
import com.lykke.matching.engine.daos.MarketOrder
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.MarketOrderMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.*
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.process.context.MarketOrderExecutionContext
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.PrintUtils
import com.lykke.matching.engine.utils.order.MessageStatusUtils
import com.lykke.matching.engine.utils.proto.toDate
import io.micrometer.core.instrument.MeterRegistry
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.BlockingQueue

@Service
class MarketOrderService @Autowired constructor(
    private val matchingEngine: MatchingEngine,
    private val executionContextFactory: ExecutionContextFactory,
    private val stopOrderBookProcessor: StopOrderBookProcessor,
    private val executionDataApplyService: ExecutionDataApplyService,
    private val matchingResultHandlingHelper: MatchingResultHandlingHelper,
    private val genericLimitOrderService: GenericLimitOrderService,
    private val assetsPairsHolder: AssetsPairsHolder,
    private val rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
    private val marketOrderValidator: MarketOrderValidator,
    private val applicationSettingsHolder: ApplicationSettingsHolder,
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
    private val messageSender: MessageSender,
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder,
    private val balancesHolder: BalancesHolder,
    registry: MeterRegistry? = null
) : AbstractService {
    companion object {
        private val LOGGER = Logger.getLogger(MarketOrderService::class.java.name)
        private val STATS_LOGGER = Logger.getLogger("${MarketOrderService::class.java.name}.stats")
    }

    private var messagesCount: Long = 0
    private var logCount = 100
    private var totalTime: Double = 0.0
    private var tradesCounter = registry?.counter("trades-counter")

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val startTime = System.nanoTime()

        val messageWrapper = genericMessageWrapper as MarketOrderMessageWrapper
        val parsedMessage = messageWrapper.parsedMessage

        messageWrapper.processedMessage = ProcessedMessage(
            messageWrapper.type.type,
            parsedMessage.timestamp.toDate().time,
            messageWrapper.messageId
        )

        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(parsedMessage.assetPairId)

        val now = Date()
        val feeInstructions: List<NewFeeInstruction>?

        if (messageProcessingStatusHolder.isTradeDisabled(assetPair)) {
            messageWrapper.writeResponse(MessageStatus.MESSAGE_PROCESSING_DISABLED)
            return
        }

        feeInstructions = NewFeeInstruction.create(parsedMessage.feesList)
        LOGGER.debug(
            "Got market order messageId: ${messageWrapper.messageId}, " +
                    "id: ${parsedMessage.id}, client: ${parsedMessage.walletId}, " +
                    "asset: ${parsedMessage.assetPairId}, volume: ${parsedMessage.volume}, " +
                    "straight: ${parsedMessage.straight}, fees: $feeInstructions"
        )

        val order = MarketOrder(
            UUID.randomUUID().toString(),
            parsedMessage.id,
            parsedMessage.assetPairId,
            parsedMessage.brokerId,
            parsedMessage.accountId,
            parsedMessage.walletId,
            BigDecimal(parsedMessage.volume),
            null,
            Processing.name,
            now,
            parsedMessage.timestamp.toDate(),
            now,
            null,
            parsedMessage.straight,
            if (parsedMessage.hasReservedLimitVolume()) BigDecimal(parsedMessage.reservedLimitVolume.value) else null,
            feeInstructions
        )
        val actualWalletVersion = balancesHolder.getBalanceVersion(
            order.clientId,
            if (order.isBuySide()) assetPair!!.quotingAssetId else assetPair!!.quotingAssetId
        )

        if (parsedMessage.walletVersion >= 0 && actualWalletVersion != parsedMessage.walletVersion) {
            LOGGER.error(
                "Invalid wallet version: ${parsedMessage.walletVersion}, actual: $actualWalletVersion"
            )
            messageWrapper.writeResponse(
                MessageStatus.INVALID_WALLET_VERSION,
                order,
                "Invalid wallet version",
                actualWalletVersion
            )
            return
        }

        try {
            marketOrderValidator.performValidation(order, getOrderBook(order), feeInstructions)
        } catch (e: OrderValidationException) {
            order.updateStatus(e.orderStatus, now)
            sendErrorNotification(messageWrapper, order, now)
            messageWrapper.writeResponse(MessageStatusUtils.toMessageStatus(order.status), order, e.message)
            return
        }

        val executionContext = executionContextFactory.create(
            messageWrapper.messageId,
            messageWrapper.id,
            MessageType.MARKET_ORDER,
            messageWrapper.processedMessage,
            mapOf(Pair(assetPair.symbol, assetPair)),
            now,
            LOGGER
        )

        val marketOrderExecutionContext = MarketOrderExecutionContext(order, executionContext)

        val matchingResult = matchingEngine.match(
            order,
            getOrderBook(order),
            messageWrapper.messageId,
            priceDeviationThreshold = if (assetPair.marketOrderPriceDeviationThreshold > BigDecimal.ZERO)
                assetPair.marketOrderPriceDeviationThreshold else applicationSettingsHolder.marketOrderPriceDeviationThreshold(
                assetPair.symbol
            ),
            executionContext = executionContext
        )
        marketOrderExecutionContext.matchingResult = matchingResult

        when (OrderStatus.valueOf(matchingResult.orderCopy.status)) {
            ReservedVolumeGreaterThanBalance,
            NoLiquidity,
            NotEnoughFunds,
            InvalidFee,
            InvalidVolumeAccuracy,
            InvalidVolume,
            InvalidValue,
            TooHighPriceDeviation -> {
                if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                    matchingResultHandlingHelper.preProcessCancelledOppositeOrders(marketOrderExecutionContext)
                    matchingResultHandlingHelper.preProcessCancelledOrdersWalletOperations(marketOrderExecutionContext)
                    matchingResultHandlingHelper.processCancelledOppositeOrders(marketOrderExecutionContext)
                    val orderBook = marketOrderExecutionContext.executionContext.orderBooksHolder
                        .getChangedOrderBookCopy(
                            marketOrderExecutionContext.order.brokerId,
                            marketOrderExecutionContext.order.assetPairId
                        )
                    matchingResult.cancelledLimitOrders.forEach {
                        orderBook.removeOrder(it.origin!!)
                    }
                }
                marketOrderExecutionContext.executionContext.marketOrderWithTrades =
                    MarketOrderWithTrades(executionContext.messageId, order)
            }
            Matched -> {
                if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                    matchingResultHandlingHelper.preProcessCancelledOppositeOrders(marketOrderExecutionContext)
                }
                if (matchingResult.uncompletedLimitOrderCopy != null) {
                    matchingResultHandlingHelper.preProcessUncompletedOppositeOrder(marketOrderExecutionContext)
                }
                marketOrderExecutionContext.ownWalletOperations = matchingResult.ownCashMovements
                val preProcessResult = try {
                    matchingResultHandlingHelper.processWalletOperations(marketOrderExecutionContext)
                    true
                } catch (e: BalanceException) {
                    order.updateStatus(NotEnoughFunds, now)
                    marketOrderExecutionContext.executionContext.marketOrderWithTrades =
                        MarketOrderWithTrades(messageWrapper.messageId, order)
                    LOGGER.error("$order: Unable to process wallet operations after matching: ${e.message}")
                    false
                }

                if (preProcessResult) {
                    matchingResult.apply()
                    executionContext.orderBooksHolder.addCompletedOrders(matchingResult.completedLimitOrders.map { it.origin!! })

                    if (matchingResult.cancelledLimitOrders.isNotEmpty()) {
                        matchingResultHandlingHelper.processCancelledOppositeOrders(marketOrderExecutionContext)
                    }
                    if (matchingResult.uncompletedLimitOrderCopy != null) {
                        matchingResultHandlingHelper.processUncompletedOppositeOrder(marketOrderExecutionContext)
                    }

                    matchingResult.skipLimitOrders.forEach { matchingResult.orderBook.put(it) }

                    marketOrderExecutionContext.executionContext.orderBooksHolder
                        .getChangedOrderBookCopy(order.brokerId, order.assetPairId)
                        .setOrderBook(!order.isBuySide(), matchingResult.orderBook)
                    marketOrderExecutionContext.executionContext.lkkTrades.addAll(matchingResult.lkkTrades)

                    marketOrderExecutionContext.executionContext.marketOrderWithTrades =
                        MarketOrderWithTrades(messageWrapper.messageId, order, matchingResult.marketOrderTrades)
                    matchingResult.limitOrdersReport?.orders?.let {
                        marketOrderExecutionContext.executionContext.addClientsLimitOrdersWithTrades(
                            it
                        )
                    }

                    tradesCounter?.increment(matchingResult.marketOrderTrades.size.toDouble())
                }
            }
            else -> {
                executionContext.error("Not handled order status: ${matchingResult.orderCopy.status}")
            }
        }

        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)
        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)
        if (!persisted) {
            LOGGER.error("$order: Unable to save result data")
            messageWrapper.writeResponse(MessageStatus.RUNTIME, order, "Unable to save result data")
            return
        }
        messageWrapper.writeResponse(
            MessageStatusUtils.toMessageStatus(order.status), order, null, balancesHolder.getBalanceVersion(
                order.clientId,
                if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId
            )
        )

        val endTime = System.nanoTime()

        messagesCount++
        totalTime += (endTime - startTime).toDouble() / logCount

        if (messagesCount % logCount == 0L) {
            STATS_LOGGER.info("Total: ${PrintUtils.convertToString(totalTime)}. ")
            totalTime = 0.0
        }
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus) {
        val messageWrapper = genericMessageWrapper as MarketOrderMessageWrapper
        messageWrapper.writeResponse(status)
    }

    private fun getOrderBook(order: MarketOrder) =
        genericLimitOrderService.getOrderBook(order.brokerId, order.assetPairId).getOrderBook(!order.isBuySide())

    private fun sendErrorNotification(
        messageWrapper: MessageWrapper,
        order: MarketOrder,
        now: Date
    ) {
        val marketOrderWithTrades = MarketOrderWithTrades(messageWrapper.messageId, order)
        rabbitSwapQueue.put(marketOrderWithTrades)
        val outgoingMessage = EventFactory.createExecutionEvent(
            messageSequenceNumberHolder.getNewValue(),
            messageWrapper.messageId,
            messageWrapper.id,
            now,
            MessageType.MARKET_ORDER,
            marketOrderWithTrades
        )
        messageSender.sendMessage(outgoingMessage)
    }
}