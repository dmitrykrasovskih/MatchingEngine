package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class CashSwap(
    val brokerId: String,
    val accountId1: String,
    val walletId1: String,
    val asset1: String,
    val volume1: String,
    val accountId2: String,
    val walletId2: String,
    val asset2: String,
    val volume2: String
) : EventPart<OutgoingMessages.CashSwap.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.CashSwap.Builder {
        val builder = OutgoingMessages.CashSwap.newBuilder()
        builder.setBrokerId(brokerId)
            .setAccountId1(accountId1)
            .setWalletId1(walletId1)
            .setAssetId1(asset1)
            .setVolume1(volume1)
            .setAccountId2(accountId2)
            .setWalletId2(walletId2)
            .setAssetId2(asset2).volume2 = volume2

        return builder
    }
}