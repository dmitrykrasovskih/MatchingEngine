package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.outgoing.messages.v2.createProtobufTimestampBuilder
import com.lykke.matching.engine.outgoing.messages.v2.enums.*
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages
import java.util.*

class Order(
    val orderType: OrderType,
    val id: String,
    val externalId: String,
    val assetPairId: String,
    val brokerId: String,
    val accountId: String,
    val walletId: String,
    val side: OrderSide,
    val volume: String,
    val remainingVolume: String?,
    val price: String?,
    val status: OrderStatus,
    val rejectReason: OrderRejectReason?,
    val statusDate: Date,
    val createdAt: Date,
    val registered: Date,
    val lastMatchTime: Date?,
    val lowerLimitPrice: String?,
    val lowerPrice: String?,
    val upperLimitPrice: String?,
    val upperPrice: String?,
    val straight: Boolean?,
    val fees: List<FeeInstruction>?,
    val trades: List<Trade>?,
    val timeInForce: OrderTimeInForce?,
    val expiryTime: Date?,
    val parentExternalId: String?,
    val childExternalId: String?
) : EventPart<OutgoingMessages.Order.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.Order.Builder {
        val builder = OutgoingMessages.Order.newBuilder()
        builder.setOrderType(OutgoingMessages.Order.OrderType.forNumber(orderType.id))
            .setId(id)
            .setExternalId(externalId)
            .setAssetPairId(assetPairId)
            .setBrokerId(brokerId)
            .setAccountId(accountId)
            .setWalletId(walletId)
            .setSide(OutgoingMessages.Order.OrderSide.forNumber(side.id))
            .volume = volume
        remainingVolume?.let {
            builder.remainingVolume = it
        }
        price?.let {
            builder.price = it
        }
        builder.status = OutgoingMessages.Order.OrderStatus.forNumber(status.id)
        rejectReason?.let {
            builder.rejectReason = rejectReason.name
        }
        builder.setStatusDate(statusDate.createProtobufTimestampBuilder())
            .setCreatedAt(createdAt.createProtobufTimestampBuilder())
            .setRegistered(registered.createProtobufTimestampBuilder())
        lastMatchTime?.let {
            builder.setLastMatchTime(it.createProtobufTimestampBuilder())
        }
        lowerLimitPrice?.let {
            builder.lowerLimitPrice = it
        }
        lowerPrice?.let {
            builder.lowerPrice = it
        }
        upperLimitPrice?.let {
            builder.upperLimitPrice = it
        }
        upperPrice?.let {
            builder.upperPrice = it
        }
        straight?.let {
            builder.straight = it
        }
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        trades?.forEach { trade ->
            builder.addTrades(trade.createGeneratedMessageBuilder())
        }
        timeInForce?.let { timeInForce ->
            builder.timeInForce = OutgoingMessages.Order.OrderTimeInForce.forNumber(timeInForce.id)
        }
        expiryTime?.let { expiryTime ->
            builder.setExpiryTime(expiryTime.createProtobufTimestampBuilder())
        }
        parentExternalId?.let {
            builder.parentExternalId = it
        }
        childExternalId?.let {
            builder.childExternalId = it
        }
        return builder
    }

}