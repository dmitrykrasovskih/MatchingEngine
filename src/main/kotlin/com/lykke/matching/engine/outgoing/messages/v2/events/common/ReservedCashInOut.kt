package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class ReservedCashInOut(
    val brokerId: String,
    val accountId: String,
    val walletId: String,
    val assetId: String,
    val reservedForOrders: String,
    val reservedForSwap: String
) : EventPart<OutgoingMessages.ReservedCashInOut.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.ReservedCashInOut.Builder {
        val builder = OutgoingMessages.ReservedCashInOut.newBuilder()
        builder.setBrokerId(brokerId)
            .setAccountId(accountId)
            .setWalletId(walletId)
            .setAssetId(assetId)
            .setReservedForOrders(reservedForOrders)
            .reservedForSwap = reservedForSwap
        return builder
    }
}