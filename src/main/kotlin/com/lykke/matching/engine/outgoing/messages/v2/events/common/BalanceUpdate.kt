package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class BalanceUpdate(val brokerId: String,
                    val accountId: String,
                    val walletId: String,
                    val assetId: String,
                    val oldBalance: String,
                    val newBalance: String,
                    val oldReserved: String,
                    val newReserved: String,
                    val version: Long) : EventPart<OutgoingMessages.BalanceUpdate.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.BalanceUpdate.Builder {
        val builder = OutgoingMessages.BalanceUpdate.newBuilder()
        builder.setBrokerId(brokerId)
                .setAccountId(accountId)
                .setWalletId(walletId)
                .setAssetId(assetId)
                .setWalletVersion(version)
                .setOldBalance(oldBalance)
                .setNewBalance(newBalance)
                .setOldReserved(oldReserved)
                .newReserved = newReserved
        return builder
    }
}