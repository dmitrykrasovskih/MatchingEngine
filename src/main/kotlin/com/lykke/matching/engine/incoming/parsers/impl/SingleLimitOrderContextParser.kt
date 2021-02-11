package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.messages.wrappers.SingleLimitOrderMessageWrapper
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.proto.toDate
import com.lykke.utils.logging.ThrottlingLogger
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class SingleLimitOrderContextParser(
    val assetsPairsHolder: AssetsPairsHolder,
    val assetsHolder: AssetsHolder,
    val applicationSettingsHolder: ApplicationSettingsHolder,
    @Qualifier("singleLimitOrderPreProcessingLogger")
    val logger: ThrottlingLogger
) : ContextParser<SingleLimitOrderParsedData, SingleLimitOrderMessageWrapper> {

    override fun parse(messageWrapper: SingleLimitOrderMessageWrapper): SingleLimitOrderParsedData {

        val context = parseMessage(messageWrapper)

        messageWrapper.context = context
        messageWrapper.processedMessage = context.processedMessage

        return SingleLimitOrderParsedData(messageWrapper, context.limitOrder.assetPairId)
    }

    private fun getContext(
        messageId: String,
        order: LimitOrder, cancelOrders: Boolean,
        processedMessage: ProcessedMessage?
    ): SingleLimitOrderContext {
        val builder = SingleLimitOrderContext.Builder()
        val assetPair = getAssetPair(order.assetPairId)

        builder.messageId(messageId)
            .limitOrder(order)
            .assetPair(assetPair)
            .baseAsset(assetPair?.let { getBaseAsset(it) })
            .quotingAsset(assetPair?.let { getQuotingAsset(it) })
            .trustedClient(getTrustedClient(builder.limitOrder.clientId))
            .limitAsset(assetPair?.let { getLimitAsset(order, assetPair) })
            .cancelOrders(cancelOrders)
            .processedMessage(processedMessage)

        return builder.build()
    }

    private fun getLimitAsset(order: LimitOrder, assetPair: AssetPair): Asset? {
        return assetsHolder.getAssetAllowNulls(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)
    }

    private fun getTrustedClient(clientId: String): Boolean {
        return applicationSettingsHolder.isTrustedClient(clientId)
    }

    fun getAssetPair(assetPairId: String): AssetPair? {
        return assetsPairsHolder.getAssetPairAllowNulls(assetPairId)
    }

    private fun getBaseAsset(assetPair: AssetPair): Asset? {
        return assetsHolder.getAssetAllowNulls(assetPair.baseAssetId)
    }

    private fun getQuotingAsset(assetPair: AssetPair): Asset? {
        return assetsHolder.getAssetAllowNulls(assetPair.quotingAssetId)
    }

    private fun parseMessage(messageWrapper: SingleLimitOrderMessageWrapper): SingleLimitOrderContext {
        val message = messageWrapper.parsedMessage
        val messageId = if (message.hasMessageId()) message.messageId.value else message.id

        val limitOrder = createOrder(message)

        val singleLimitOrderContext = getContext(
            messageId,
            limitOrder,
            if (message.hasCancelAllPreviousLimitOrders()) message.cancelAllPreviousLimitOrders.value else false,
            ProcessedMessage(messageWrapper.type.type, message.timestamp.toDate().time, messageId)
        )

        logger.info("Got limit order  messageId: $messageId, id: ${message.id}, client ${message.walletId}")

        return singleLimitOrderContext
    }

    private fun createOrder(message: GrpcIncomingMessages.LimitOrder): LimitOrder {
        val type = LimitOrderType.getByExternalId(message.type.number)
        val status = when (type) {
            LimitOrderType.LIMIT -> OrderStatus.InOrderBook
            LimitOrderType.STOP_LIMIT -> OrderStatus.Pending
        }
        val feeInstructions = NewLimitOrderFeeInstruction.create(message.feesList)
        return LimitOrder(
            UUID.randomUUID().toString(),
            message.id,
            message.assetPairId,
            message.brokerId,
            message.accountId,
            message.walletId,
            BigDecimal(message.volume),
            if (message.hasPrice()) BigDecimal(message.price.value) else BigDecimal.ZERO,
            status.name,
            null,
            message.timestamp.toDate(),
            null,
            BigDecimal(message.volume),
            null,
            fees = feeInstructions,
            type = type,
            lowerLimitPrice = if (message.hasLowerLimitPrice()) BigDecimal(message.lowerLimitPrice.value) else null,
            lowerPrice = if (message.hasLowerPrice()) BigDecimal(message.lowerPrice.value) else null,
            upperLimitPrice = if (message.hasUpperLimitPrice()) BigDecimal(message.upperLimitPrice.value) else null,
            upperPrice = if (message.hasUpperPrice()) BigDecimal(message.upperPrice.value) else null,
            previousExternalId = null,
            timeInForce = OrderTimeInForce.getByExternalId(message.timeInForce.number),
            expiryTime = if (message.hasExpiryTime()) message.expiryTime.toDate() else null,
            parentOrderExternalId = null,
            childOrderExternalId = null
        )
    }
}