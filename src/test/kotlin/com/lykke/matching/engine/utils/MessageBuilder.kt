package com.lykke.matching.engine.utils

import com.google.protobuf.BoolValue
import com.google.protobuf.StringValue
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.v2.FeeInstruction
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.messages.wrappers.*
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.matching.engine.messages.wrappers.socket.MultiLimitOrderCancelMessageWrapper
import com.lykke.matching.engine.order.OrderCancelMode
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.validators.impl.OrderValidationResult
import com.lykke.matching.engine.utils.proto.createProtobufTimestampBuilder
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import com.myjetwallet.messages.incoming.socket.SocketIncomingMessages
import java.math.BigDecimal
import java.util.*

class MessageBuilder(
    private var singleLimitOrderContextParser: SingleLimitOrderContextParser,
    private val cashInOutContextParser: CashInOutContextParser,
    private val cashTransferContextParser: CashTransferContextParser,
    private val limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData, LimitOrderCancelMessageWrapper>,
    private val limitOrderMassCancelOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData, LimitOrderMassCancelMessageWrapper>
) {
    companion object {
        fun buildLimitOrder(
            uid: String = UUID.randomUUID().toString(),
            assetId: String = "EURUSD",
            clientId: String = "Client1",
            price: Double = 100.0,
            registered: Date = Date(),
            status: String = OrderStatus.InOrderBook.name,
            volume: Double = 1000.0,
            type: LimitOrderType = LimitOrderType.LIMIT,
            lowerLimitPrice: Double? = null,
            lowerPrice: Double? = null,
            upperLimitPrice: Double? = null,
            upperPrice: Double? = null,
            reservedVolume: Double? = null,
            fees: List<NewLimitOrderFeeInstruction> = listOf(),
            previousExternalId: String? = null,
            timeInForce: OrderTimeInForce? = null,
            expiryTime: Date? = null
        ): LimitOrder =
            LimitOrder(
                uid,
                uid,
                assetId,
                "",
                "",
                clientId,
                BigDecimal.valueOf(volume),
                BigDecimal.valueOf(price),
                status,
                registered,
                registered,
                registered,
                BigDecimal.valueOf(volume),
                null,
                reservedVolume?.toBigDecimal(),
                fees,
                type,
                lowerLimitPrice?.toBigDecimal(),
                lowerPrice?.toBigDecimal(),
                upperLimitPrice?.toBigDecimal(),
                upperPrice?.toBigDecimal(),
                previousExternalId,
                timeInForce,
                expiryTime,
                null,
                null
            )

        fun buildMarketOrderWrapper(order: MarketOrder): MarketOrderMessageWrapper {
            val builder = GrpcIncomingMessages.MarketOrder.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setTimestamp(order.createdAt.createProtobufTimestampBuilder())
                .setWalletId(order.clientId)
                .setWalletVersion(-1)
                .setAssetPairId(order.assetPairId)
                .setVolume(order.volume.toPlainString())
                .setStraight(order.straight)
            order.fees?.forEach {
                builder.addFees(buildFee(it))
            }
            return MarketOrderMessageWrapper(
                builder.build(),
                TestStreamObserver()
            )
        }

        fun buildFee(fee: FeeInstruction): GrpcIncomingMessages.Fee {
            val builder = GrpcIncomingMessages.Fee.newBuilder().setType(fee.type.externalId)
            fee.size?.let {
                builder.size = StringValue.of(it.toPlainString())
            }
            fee.sourceWalletId?.let {
                builder.setSourceWalletId(StringValue.of(it))
            }
            fee.targetWalletId?.let {
                builder.setTargetWalletId(StringValue.of(it))
            }
            fee.sizeType?.let {
                builder.setSizeType(it.externalId)
            }
            if (fee is NewFeeInstruction) {
                builder.addAllAssetId(fee.assetIds)
            }
            return builder.build()
        }

        fun buildNewLimitOrderFee(fee: NewLimitOrderFeeInstruction): GrpcIncomingMessages.LimitOrderFee {
            val builder = GrpcIncomingMessages.LimitOrderFee.newBuilder().setType(fee.type.externalId)
            fee.size?.let {
                builder.takerSize = StringValue.of(it.toPlainString())
            }
            fee.sizeType?.let {
                builder.takerSizeType = it.externalId
            }
            fee.makerSize?.let {
                builder.makerSize = StringValue.of(it.toPlainString())
            }
            fee.makerSizeType?.let {
                builder.makerSizeType = it.externalId
            }
            fee.sourceWalletId?.let {
                builder.setSourceWalletId(StringValue.of(it))
            }
            fee.targetWalletId?.let {
                builder.setTargetWalletId(StringValue.of(it))
            }
            builder.addAllAssetId(fee.assetIds)
            return builder.build()
        }

        fun buildMarketOrder(
            rowKey: String = UUID.randomUUID().toString(),
            assetId: String = "EURUSD",
            clientId: String = "Client1",
            registered: Date = Date(),
            status: String = OrderStatus.InOrderBook.name,
            straight: Boolean = true,
            volume: Double = 1000.0,
            reservedVolume: Double? = null,
            fees: List<NewFeeInstruction> = listOf()
        ): MarketOrder =
            MarketOrder(
                rowKey, rowKey, assetId, "", "", clientId,
                BigDecimal.valueOf(volume), null, status, registered, registered, Date(),
                null, straight,
                reservedVolume?.toBigDecimal(),
                fees = fees
            )

        fun buildMultiLimitOrderWrapper(
            pair: String,
            clientId: String,
            orders: List<IncomingLimitOrder>,
            cancel: Boolean = true,
            cancelMode: OrderCancelMode? = null
        ): MessageWrapper {
            return MultiLimitOrderMessageWrapper(
                buildMultiLimitOrder(
                    pair, clientId,
                    orders,
                    cancel,
                    cancelMode
                ), TestStreamObserver()
            )
        }

        private fun buildMultiLimitOrder(
            assetPairId: String,
            clientId: String,
            orders: List<IncomingLimitOrder>,
            cancel: Boolean,
            cancelMode: OrderCancelMode?
        ): GrpcIncomingMessages.MultiLimitOrder {
            val multiOrderBuilder = GrpcIncomingMessages.MultiLimitOrder.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setTimestamp(Date().createProtobufTimestampBuilder())
                .setWalletId(clientId)
                .setAssetPairId(assetPairId)
                .setCancelAllPreviousLimitOrders(BoolValue.of(cancel))
            cancelMode?.let {
                multiOrderBuilder.cancelMode = GrpcIncomingMessages.MultiLimitOrder.CancelMode.forNumber(it.externalId)
            }
            orders.forEach { order ->
                val orderBuilder = GrpcIncomingMessages.MultiLimitOrder.Order.newBuilder()
                    .setVolume(order.volume.toString())
                order.price?.let { orderBuilder.price = it.toString() }
                order.feeInstructions.forEach { orderBuilder.addFees(buildNewLimitOrderFee(it)) }
                orderBuilder.id = order.uid
                order.oldUid?.let { orderBuilder.oldId = StringValue.of(order.oldUid) }
                order.timeInForce?.let {
                    orderBuilder.timeInForce = GrpcIncomingMessages.OrderTimeInForce.forNumber(it.externalId)
                }
                order.expiryTime?.let { orderBuilder.expiryTime = it.createProtobufTimestampBuilder().build() }
                multiOrderBuilder.addOrders(orderBuilder.build())
            }
            return multiOrderBuilder.build()
        }

        fun buildMultiLimitOrderCancelWrapper(clientId: String, assetPairId: String, isBuy: Boolean): MessageWrapper =
            MultiLimitOrderCancelMessageWrapper(
                SocketIncomingMessages.MultiLimitOrderCancel.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setTimestamp(Date().createProtobufTimestampBuilder())
                    .setWalletId(clientId)
                    .setAssetPairId(assetPairId)
                    .setIsBuy(isBuy).build(), TestStreamObserver()
            )

        fun buildFeeInstructions(
            type: FeeType? = null,
            sizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
            size: Double? = null,
            sourceClientId: String? = null,
            targetClientId: String? = null,
            assetIds: List<String> = emptyList()
        ): List<NewFeeInstruction> {
            return if (type == null) listOf()
            else return listOf(
                NewFeeInstruction(
                    type, sizeType,
                    if (size != null) BigDecimal.valueOf(size) else null,
                    sourceClientId, targetClientId, assetIds
                )
            )
        }

        fun buildFeeInstruction(
            type: FeeType? = null,
            sizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
            size: Double? = null,
            sourceClientId: String? = null,
            targetClientId: String? = null,
            assetIds: List<String> = emptyList()
        ): NewFeeInstruction? {
            return if (type == null) null
            else return NewFeeInstruction(
                type, sizeType,
                if (size != null) BigDecimal.valueOf(size) else null,
                sourceClientId, targetClientId, assetIds
            )
        }

        fun buildLimitOrderFeeInstruction(
            type: FeeType? = null,
            takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
            takerSize: Double? = null,
            makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
            makerSize: Double? = null,
            sourceClientId: String? = null,
            targetClientId: String? = null
        ): LimitOrderFeeInstruction? {
            return if (type == null) null
            else return LimitOrderFeeInstruction(
                type, takerSizeType,
                if (takerSize != null) BigDecimal.valueOf(takerSize) else null,
                makerSizeType,
                if (makerSize != null) BigDecimal.valueOf(makerSize) else null,
                sourceClientId,
                targetClientId
            )
        }

        fun buildLimitOrderFeeInstructions(
            type: FeeType? = null,
            takerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
            takerSize: Double? = null,
            makerSizeType: FeeSizeType? = FeeSizeType.PERCENTAGE,
            makerSize: Double? = null,
            sourceClientId: String? = null,
            targetClientId: String? = null,
            assetIds: List<String> = emptyList(),
            makerFeeModificator: Double? = null
        ): List<NewLimitOrderFeeInstruction> {
            return if (type == null) listOf()
            else return listOf(
                NewLimitOrderFeeInstruction(
                    type, takerSizeType,
                    if (takerSize != null) BigDecimal.valueOf(takerSize) else null,
                    makerSizeType,
                    if (makerSize != null) BigDecimal.valueOf(makerSize) else null,
                    sourceClientId, targetClientId, assetIds,
                    if (makerFeeModificator != null) BigDecimal.valueOf(makerFeeModificator) else null
                )
            )
        }
    }

    fun buildTransferWrapper(
        fromClientId: String,
        toClientId: String,
        assetId: String,
        amount: Double,
        overdraftLimit: Double,
        businessId: String = UUID.randomUUID().toString()
    ): MessageWrapper {
        return cashTransferContextParser.parse(
            CashTransferOperationMessageWrapper(
                GrpcIncomingMessages.CashTransferOperation.newBuilder()
                    .setId(businessId)
                    .setFromWalletId(fromClientId)
                    .setToWalletId(toClientId)
                    .setAssetId(assetId)
                    .setVolume(amount.toString())
                    .setOverdraftLimit(StringValue.of(overdraftLimit.toString()))
                    .setTimestamp(Date().createProtobufTimestampBuilder()).build(), TestStreamObserver()
            )
        ).messageWrapper
    }

    fun buildCashInOutWrapper(
        clientId: String, assetId: String, amount: Double, businessId: String = UUID.randomUUID().toString(),
        fees: List<NewFeeInstruction> = listOf()
    ): MessageWrapper {
        val builder = GrpcIncomingMessages.CashInOutOperation.newBuilder()
            .setId(businessId)
            .setWalletId(clientId)
            .setAssetId(assetId)
            .setVolume(amount.toString())
            .setTimestamp(Date().createProtobufTimestampBuilder())
        fees.forEach {
            builder.addFees(buildFee(it))
        }

        return cashInOutContextParser.parse(
            CashInOutOperationMessageWrapper(
                builder.build(),
                TestStreamObserver()
            )
        ).messageWrapper
    }

    fun buildLimitOrderCancelWrapper(uid: String) = buildLimitOrderCancelWrapper(listOf(uid))

    fun buildLimitOrderCancelWrapper(uids: List<String>): MessageWrapper {
        val parsedData = limitOrderCancelOperationContextParser.parse(
            LimitOrderCancelMessageWrapper(
                GrpcIncomingMessages.LimitOrderCancel.newBuilder()
                    .setId(UUID.randomUUID().toString()).addAllLimitOrderId(uids).build(), TestStreamObserver()
            )
        )
        return parsedData.messageWrapper
    }

    fun buildLimitOrderMassCancelWrapper(
        clientId: String,
        assetPairId: String? = null,
        isBuy: Boolean? = null
    ): MessageWrapper {
        val builder = SocketIncomingMessages.LimitOrderMassCancel.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setWalletId(StringValue.of(clientId))
        assetPairId?.let {
            builder.setAssetPairId(StringValue.of(it))
        }
        isBuy?.let {
            builder.setIsBuy(BoolValue.of(it))
        }

        val messageWrapper = LimitOrderMassCancelMessageWrapper(builder.build(), TestStreamObserver())
        return limitOrderMassCancelOperationContextParser.parse(messageWrapper).messageWrapper
    }

    fun buildLimitOrderWrapper(
        order: LimitOrder,
        cancel: Boolean = false
    ): SingleLimitOrderMessageWrapper {
        val builder = GrpcIncomingMessages.LimitOrder.newBuilder()
            .setId(order.externalId)
            .setTimestamp(order.createdAt.createProtobufTimestampBuilder())
            .setWalletId(order.clientId)
            .setAssetPairId(order.assetPairId)
            .setVolume(order.volume.toPlainString())
            .setCancelAllPreviousLimitOrders(BoolValue.of(cancel))
            .setType(GrpcIncomingMessages.LimitOrder.LimitOrderType.forNumber(order.type!!.externalId))
            .setWalletVersion(-1)
        if (order.type == LimitOrderType.LIMIT) {
            builder.price = StringValue.of(order.price.toPlainString())
        }
        order.fees?.forEach {
            builder.addFees(buildNewLimitOrderFee(it as NewLimitOrderFeeInstruction))
        }
        order.lowerLimitPrice?.let { builder.setLowerLimitPrice(StringValue.of(it.toPlainString())) }
        order.lowerPrice?.let { builder.setLowerPrice(StringValue.of(it.toPlainString())) }
        order.upperLimitPrice?.let { builder.setUpperLimitPrice(StringValue.of(it.toPlainString())) }
        order.upperPrice?.let { builder.setUpperPrice(StringValue.of(it.toPlainString())) }
        order.expiryTime?.let { builder.setExpiryTime(it.createProtobufTimestampBuilder()) }
        order.timeInForce?.let { builder.setTimeInForce(GrpcIncomingMessages.OrderTimeInForce.forNumber(it.externalId)) }
        val messageWrapper = singleLimitOrderContextParser
            .parse(
                SingleLimitOrderMessageWrapper(
                    builder.build(),
                    TestStreamObserver()
                )
            )
            .messageWrapper as SingleLimitOrderMessageWrapper

        val singleLimitContext = messageWrapper.context as SingleLimitOrderContext
        singleLimitContext.validationResult = OrderValidationResult(true)

        return messageWrapper
    }
}