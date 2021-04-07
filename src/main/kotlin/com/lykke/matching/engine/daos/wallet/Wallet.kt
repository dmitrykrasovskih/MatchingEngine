package com.lykke.matching.engine.daos.wallet

import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.util.*

class Wallet {
    val brokerId: String
    val accountId: String
    val clientId: String
    val balances: MutableMap<String, AssetBalance> = HashMap()

    constructor(brokerId: String, accountId: String, clientId: String) {
        this.brokerId = brokerId
        this.accountId = accountId
        this.clientId = clientId
    }

    constructor(brokerId: String, accountId: String, clientId: String, balances: List<AssetBalance>) {
        this.brokerId = brokerId
        this.accountId = accountId
        this.clientId = clientId
        balances.forEach {
            this.balances[it.asset] = it
        }
    }

    fun setBalance(asset: String, balance: BigDecimal) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(clientId, asset, balance, ZERO, ZERO, brokerId, accountId, 1)
        } else {
            oldBalance.balance = balance
        }
    }

    fun setReservedForOrdersBalance(asset: String, reservedForOrdersBalance: BigDecimal) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(
                clientId,
                asset,
                reservedForOrdersBalance,
                reservedForOrdersBalance,
                ZERO,
                brokerId,
                accountId,
                1
            )
        } else {
            oldBalance.reserved = reservedForOrdersBalance
        }
    }

    fun setReservedForSwapBalance(asset: String, reservedForSwapBalance: BigDecimal) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(
                clientId,
                asset,
                reservedForSwapBalance,
                ZERO,
                reservedForSwapBalance,
                brokerId,
                accountId,
                1
            )
        } else {
            oldBalance.reservedForSwap = reservedForSwapBalance
        }
    }

    fun increaseWalletVersion(asset: String) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(clientId, asset, ZERO, ZERO, ZERO, brokerId, accountId, 1)
        } else {
            oldBalance.version++
        }
    }
}