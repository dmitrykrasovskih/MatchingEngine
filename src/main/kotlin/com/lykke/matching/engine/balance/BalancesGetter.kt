package com.lykke.matching.engine.balance

import java.math.BigDecimal

interface BalancesGetter {
    fun getAvailableBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
    fun getAvailableReservedBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
    fun getReservedForOrdersBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
    fun getReservedFoSwapBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
    fun getReservedTotalBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
}