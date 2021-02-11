package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.MultiLimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.MessageStatus.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.MultiLimitOrderMessageWrapper
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.services.utils.MultiOrderFilter
import com.lykke.matching.engine.utils.proto.toDate
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import org.apache.log4j.Logger
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.*

@Service
class MultiLimitOrderService(
    private val executionContextFactory: ExecutionContextFactory,
    private val genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
    private val stopOrderBookProcessor: StopOrderBookProcessor,
    private val executionDataApplyService: ExecutionDataApplyService,
    private val previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
    private val assetsHolder: AssetsHolder,
    private val assetsPairsHolder: AssetsPairsHolder,
    private val balancesHolder: BalancesHolder,
    private val applicationSettingsHolder: ApplicationSettingsHolder,
    private val messageProcessingStatusHolder: MessageProcessingStatusHolder
) : AbstractService {

    companion object {
        private val LOGGER = Logger.getLogger(MultiLimitOrderService::class.java.name)
    }

    override fun processMessage(genericMessageWrapper: MessageWrapper) {
        val messageWrapper = genericMessageWrapper as MultiLimitOrderMessageWrapper
        val message = messageWrapper.parsedMessage
        val assetPair = assetsPairsHolder.getAssetPairAllowNulls(message.assetPairId)
        if (assetPair == null) {
            LOGGER.info("Unable to process message (${messageWrapper.messageId}): unknown asset pair ${message.assetPairId}")
            messageWrapper.writeResponse(UNKNOWN_ASSET)
            return
        }

        if (messageProcessingStatusHolder.isTradeDisabled(assetPair)) {
            messageWrapper.writeResponse(MESSAGE_PROCESSING_DISABLED)
            return
        }

        val isTrustedClient = applicationSettingsHolder.isTrustedClient(message.walletId)

        val multiLimitOrder = readMultiLimitOrder(messageWrapper.messageId, message, isTrustedClient, assetPair)
        val now = Date()

        val executionContext = executionContextFactory.create(
            messageWrapper.messageId,
            messageWrapper.id,
            MessageType.MULTI_LIMIT_ORDER,
            messageWrapper.processedMessage,
            mapOf(Pair(assetPair.symbol, assetPair)),
            now,
            LOGGER
        )

        previousLimitOrdersProcessor.cancelAndReplaceOrders(
            multiLimitOrder.clientId,
            multiLimitOrder.assetPairId,
            multiLimitOrder.cancelAllPreviousLimitOrders,
            multiLimitOrder.cancelBuySide,
            multiLimitOrder.cancelSellSide,
            multiLimitOrder.buyReplacements,
            multiLimitOrder.sellReplacements,
            executionContext
        )

        val processedOrders = genericLimitOrdersProcessor.processOrders(multiLimitOrder.orders, executionContext)
        stopOrderBookProcessor.checkAndExecuteStopLimitOrders(executionContext)
        val persisted = executionDataApplyService.persistAndSendEvents(messageWrapper, executionContext)

        if (!persisted) {
            val errorMessage = "Unable to save result data"
            LOGGER.error("$errorMessage (multi limit order id ${multiLimitOrder.messageUid})")

            messageWrapper.writeResponse(RUNTIME, errorMessage)
            return
        }
        messageWrapper.writeResponse(OK, null, processedOrders)
    }

    override fun writeResponse(genericMessageWrapper: MessageWrapper, status: MessageStatus){
        val messageWrapper = genericMessageWrapper as MultiLimitOrderMessageWrapper
        messageWrapper.writeResponse(status)
    }

    private fun readMultiLimitOrder(
        messageId: String,
        message: GrpcIncomingMessages.MultiLimitOrder,
        isTrustedClient: Boolean,
        assetPair: AssetPair
    ): MultiLimitOrder {
        LOGGER.debug(
            "Got ${if (!isTrustedClient) "client " else ""} multi limit order id: ${message.id}, " +
                    (if (messageId != message.id) "messageId: $messageId, " else "") +
                    "client ${message.walletId}, " +
                    "assetPair: ${message.assetPairId}, " +
                    "ordersCount: ${message.ordersCount}, " +
                    (if (message.hasCancelAllPreviousLimitOrders()) "cancelPrevious: ${message.cancelAllPreviousLimitOrders.value}, " else "") +
                    (if (message.cancelMode != null) "cancelMode: ${message.cancelMode}" else "")
        )

        val clientId = message.walletId
        val messageUid = message.id
        val assetPairId = message.assetPairId
        val cancelAllPreviousLimitOrders =
            if (message.hasCancelAllPreviousLimitOrders()) message.cancelAllPreviousLimitOrders.value else false
        val cancelMode =
            if (message.cancelMode != null) OrderCancelMode.getByExternalId(message.cancelMode.number) else OrderCancelMode.NOT_EMPTY_SIDE
        val now = Date()
        var cancelBuySide = cancelMode == OrderCancelMode.BUY_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES
        var cancelSellSide = cancelMode == OrderCancelMode.SELL_SIDE || cancelMode == OrderCancelMode.BOTH_SIDES

        val buyReplacements = mutableMapOf<String, LimitOrder>()
        val sellReplacements = mutableMapOf<String, LimitOrder>()

        val baseAssetAvailableBalance =
            balancesHolder.getAvailableBalance(message.brokerId, message.accountId, clientId, assetPair.baseAssetId)
        val quotingAssetAvailableBalance =
            balancesHolder.getAvailableBalance(message.brokerId, message.accountId, clientId, assetPair.quotingAssetId)

        val filter = MultiOrderFilter(
            isTrustedClient,
            baseAssetAvailableBalance,
            quotingAssetAvailableBalance,
            assetsHolder.getAsset(assetPair.quotingAssetId).accuracy,
            now,
            message.ordersList.size,
            LOGGER
        )

        message.ordersList.forEach { currentOrder ->
            if (!isTrustedClient) {
                LOGGER.debug("Incoming limit order (message id: $messageId): ${getIncomingOrderInfo(currentOrder)}")
            }
            val type = LimitOrderType.LIMIT
            val status = OrderStatus.InOrderBook
            val price = if (currentOrder.price != null) BigDecimal(currentOrder.price) else BigDecimal.ZERO
            val lowerLimitPrice = null
            val lowerPrice = null
            val upperLimitPrice = null
            val upperPrice = null
            val feeInstructions = NewLimitOrderFeeInstruction.create(currentOrder.feesList)
            val previousExternalId = if (currentOrder.hasOldId()) currentOrder.oldId.value else null

            val order = LimitOrder(
                UUID.randomUUID().toString(),
                currentOrder.id,
                message.assetPairId,
                message.brokerId,
                message.accountId,
                message.walletId,
                BigDecimal(currentOrder.volume),
                price,
                status.name,
                now,
                message.timestamp.toDate(),
                now,
                BigDecimal(currentOrder.volume),
                null,
                fees = feeInstructions,
                type = type,
                lowerLimitPrice = lowerLimitPrice,
                lowerPrice = lowerPrice,
                upperLimitPrice = upperLimitPrice,
                upperPrice = upperPrice,
                previousExternalId = previousExternalId,
                timeInForce = if (currentOrder.timeInForce != null) OrderTimeInForce.getByExternalId(currentOrder.timeInForce.number) else null,
                expiryTime = if (currentOrder.hasExpiryTime()) currentOrder.expiryTime.toDate() else null,
                parentOrderExternalId = null,
                childOrderExternalId = null
            )

            filter.checkAndAdd(order)
            previousExternalId?.let {
                (if (order.isBuySide()) buyReplacements else sellReplacements)[it] = order
            }

            if (cancelAllPreviousLimitOrders && cancelMode == OrderCancelMode.NOT_EMPTY_SIDE) {
                if (order.isBuySide()) {
                    cancelBuySide = true
                } else {
                    cancelSellSide = true
                }
            }
        }

        return MultiLimitOrder(
            messageUid,
            clientId,
            assetPairId,
            filter.getResult(),
            cancelAllPreviousLimitOrders,
            cancelBuySide,
            cancelSellSide,
            cancelMode,
            buyReplacements,
            sellReplacements
        )
    }

    private fun getIncomingOrderInfo(incomingOrder: GrpcIncomingMessages.MultiLimitOrder.Order): String {
        return "id: ${incomingOrder.id}" +
                ", volume: ${incomingOrder.volume}" +
                ", price: ${incomingOrder.price}" +
                (if (incomingOrder.hasOldId()) ", oldUid: ${incomingOrder.oldId}" else "") +
                (if (incomingOrder.timeInForce != null) ", timeInForce: ${incomingOrder.timeInForce}" else "") +
                (if (incomingOrder.hasExpiryTime()) ", expiryTime: ${incomingOrder.expiryTime}" else "") +
                (if (incomingOrder.feesCount > 0) ", fees: ${
                    incomingOrder.feesList.asSequence().map { getIncomingFeeInfo(it) }.joinToString(", ")
                }" else "")
    }

    private fun getIncomingFeeInfo(incomingFee: GrpcIncomingMessages.LimitOrderFee): String {
        return "type: ${incomingFee.type}, " +
                (if (incomingFee.hasMakerSize()) ", makerSize: ${incomingFee.makerSize}" else "") +
                (if (incomingFee.hasTakerSize()) ", takerSize: ${incomingFee.takerSize}" else "") +
                (if (incomingFee.hasSourceWalletId()) ", sourceClientId: ${incomingFee.sourceWalletId.value}" else "") +
                (if (incomingFee.hasTargetWalletId()) ", targetClientId: ${incomingFee.targetWalletId}" else "") +
                (if (incomingFee.makerSizeType != null) ", makerSizeType: ${incomingFee.makerSizeType}" else "") +
                (if (incomingFee.takerSizeType != null) ", takerSizeType: ${incomingFee.takerSizeType}" else "") +
                (if (incomingFee.hasMakerFeeModificator()) ", makerFeeModificator: ${incomingFee.makerFeeModificator}" else "") +
                (if (incomingFee.assetIdCount > 0) ", assetIds: ${incomingFee.assetIdList}}" else "")
    }
}