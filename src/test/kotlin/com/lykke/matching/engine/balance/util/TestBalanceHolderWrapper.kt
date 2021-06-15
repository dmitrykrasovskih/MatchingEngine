package com.lykke.matching.engine.balance.util

import com.lykke.matching.engine.holders.BalancesHolder
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal

class TestBalanceHolderWrapper @Autowired constructor(
    private val balancesHolder: BalancesHolder
) {

    fun updateBalance(clientId: String, assetId: String, balance: Double) {
        balancesHolder.updateBalance(null, null, "", "", clientId, assetId, BigDecimal.valueOf(balance))
    }

    fun updateReservedBalance(clientId: String, assetId: String, reservedBalance: Double, skip: Boolean = false) {
        balancesHolder.updateReservedBalance(
            null,
            null,
            "",
            "",
            clientId,
            assetId,
            BigDecimal.valueOf(reservedBalance),
            skip
        )
    }
}