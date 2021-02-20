package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.outgoing.messages.v2.createProtobufTimestampBuilder
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages
import java.util.*

class Trade(
    val tradeId: String,
    val baseAssetId: String,
    val baseVolume: String,
    val price: String,
    val timestamp: Date,
    val oppositeOrderId: String,
    val oppositeExternalOrderId: String,
    val oppositeWalletId: String,
    val quotingAssetId: String,
    val quotingVolume: String,
    val index: Int,
    val absoluteSpread: String?,
    val relativeSpread: String?,
    val role: TradeRole,
    val fees: List<FeeTransfer>?
) : EventPart<OutgoingMessages.Order.Trade.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.Order.Trade.Builder {
        val builder = OutgoingMessages.Order.Trade.newBuilder()
        builder.setTradeId(tradeId)
            .setBaseAssetId(baseAssetId)
            .setBaseVolume(baseVolume)
            .setPrice(price)
            .setTimestamp(timestamp.createProtobufTimestampBuilder())
            .setOppositeOrderId(oppositeOrderId)
            .setOppositeExternalOrderId(oppositeExternalOrderId)
            .setOppositeWalletId(oppositeWalletId)
            .setQuotingAssetId(quotingAssetId)
            .setQuotingVolume(quotingVolume)
            .setIndex(index)
            .role = OutgoingMessages.Order.Trade.TradeRole.forNumber(role.id)
        absoluteSpread?.let {
            builder.setAbsoluteSpread(it)
        }
        relativeSpread?.let {
            builder.setRelativeSpread(it)
        }
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        return builder
    }

}