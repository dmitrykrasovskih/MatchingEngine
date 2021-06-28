package com.lykke.matching.engine.order.transaction

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.holders.BalancesHolder
import java.math.BigDecimal
import java.math.BigDecimal.ZERO

class CurrentTransactionBalancesHolder(private val balancesHolder: BalancesHolder) {

    private val changedBalancesByClientIdAndAssetId = mutableMapOf<String, MutableMap<String, AssetBalance>>()
    private val changedWalletsByClientId = mutableMapOf<String, Wallet>()

    fun updateBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String,
        balance: BigDecimal
    ) {
        val walletAssetBalance = getWalletAssetBalance(brokerId, accountId, clientId, assetId)
        walletAssetBalance.assetBalance.brokerId = brokerId
        walletAssetBalance.assetBalance.accountId = accountId
        walletAssetBalance.assetBalance.balance = balance
    }

    fun updateReservedBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String,
        balance: BigDecimal
    ) {
        val walletAssetBalance = getWalletAssetBalance(brokerId, accountId, clientId, assetId)
        walletAssetBalance.assetBalance.brokerId = brokerId
        walletAssetBalance.assetBalance.accountId = accountId
        walletAssetBalance.assetBalance.reserved = balance
    }

    fun persistenceData(): BalancesData {
        return BalancesData(
            changedWalletsByClientId.values,
            changedBalancesByClientIdAndAssetId.flatMap { it.value.values })
    }

    fun apply() {
        balancesHolder.setWallets(changedWalletsByClientId.values)
    }

    fun getWalletAssetBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): WalletAssetBalance {
        val wallet = changedWalletsByClientId.getOrPut(clientId) {
            copyWallet(balancesHolder.wallets[clientId]) ?: Wallet(brokerId, accountId, clientId)
        }
        val assetBalance = changedBalancesByClientIdAndAssetId
            .getOrPut(clientId) {
                mutableMapOf()
            }
            .getOrPut(assetId) {
                wallet.balances.getOrPut(assetId) {
                    AssetBalance(
                        clientId,
                        assetId,
                        ZERO,
                        ZERO,
                        ZERO,
                        brokerId,
                        accountId,
                        1
                    )
                }
            }
        return WalletAssetBalance(wallet, assetBalance)
    }

    fun getChangedCopyOrOriginalAssetBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): AssetBalance {
        return (changedWalletsByClientId[clientId] ?: balancesHolder.wallets[clientId] ?: Wallet(
            brokerId,
            accountId,
            clientId
        )).balances[assetId]
            ?: AssetBalance(clientId, assetId, ZERO, ZERO, ZERO, brokerId, accountId, 1)
    }

    private fun copyWallet(wallet: Wallet?): Wallet? {
        if (wallet == null) {
            return null
        }
        return Wallet(
            wallet.brokerId,
            wallet.accountId,
            wallet.clientId,
            wallet.balances.values.map { copyAssetBalance(it) })
    }

    private fun copyAssetBalance(assetBalance: AssetBalance): AssetBalance {
        return AssetBalance(
            assetBalance.clientId,
            assetBalance.asset,
            assetBalance.balance,
            assetBalance.reserved,
            assetBalance.reservedForSwap,
            assetBalance.brokerId,
            assetBalance.accountId,
            assetBalance.version
        )
    }
}

class WalletAssetBalance(val wallet: Wallet, val assetBalance: AssetBalance)