package com.lykke.matching.engine.balance

import java.math.BigDecimal

interface BalancesGetter {
    fun getAvailableBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
    fun getAvailableReservedBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
    fun getReservedBalance(brokerId: String, accountId: String, clientId: String, assetId: String): BigDecimal
}