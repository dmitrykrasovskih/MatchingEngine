package com.lykke.matching.engine.daos.wallet

import java.math.BigDecimal
import java.util.HashMap

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
            balances[asset] = AssetBalance(clientId, asset, balance)
        } else {
            oldBalance.balance = balance
        }
    }

    fun setReservedBalance(asset: String, reservedBalance: BigDecimal) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(clientId, asset, reservedBalance, reservedBalance)
        } else {
            oldBalance.reserved = reservedBalance
        }
    }

    fun increaseWalletVersion(asset: String) {
        val oldBalance = balances[asset]
        if (oldBalance == null) {
            balances[asset] = AssetBalance(clientId, asset, BigDecimal.ZERO, BigDecimal.ZERO, brokerId, accountId, 1)
        } else {
            oldBalance.version++
        }
    }
}