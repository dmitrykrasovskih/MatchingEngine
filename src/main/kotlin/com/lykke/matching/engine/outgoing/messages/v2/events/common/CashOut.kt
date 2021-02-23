package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class CashOut(
    val brokerId: String,
    val accountId: String,
    val walletId: String,
    val assetId: String,
    val volume: String,
    val fees: List<Fee>?
) : EventPart<OutgoingMessages.CashOut.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.CashOut.Builder {
        val builder = OutgoingMessages.CashOut.newBuilder()
        builder.setBrokerId(brokerId)
            .setAccountId(accountId)
            .setWalletId(walletId)
            .setAssetId(assetId)
            .volume = volume
        fees?.forEach { fee ->
            builder.addFees(fee.createGeneratedMessageBuilder())
        }
        return builder
    }
}