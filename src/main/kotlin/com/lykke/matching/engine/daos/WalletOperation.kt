package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal

data class WalletOperation(
    val brokerId: String,
    val accountId: String,
    val clientId: String,
    val assetId: String,
    val amount: BigDecimal,
    val reservedAmount: BigDecimal = BigDecimal.ZERO,
    val reservedForSwapAmount: BigDecimal = BigDecimal.ZERO
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as WalletOperation

        if (brokerId != other.brokerId) return false
        if (accountId != other.accountId) return false
        if (clientId != other.clientId) return false
        if (assetId != other.assetId) return false
        if (!NumberUtils.equalsIgnoreScale(amount, other.amount)) return false
        if (!NumberUtils.equalsIgnoreScale(reservedAmount, other.reservedAmount)) return false
        if (!NumberUtils.equalsIgnoreScale(reservedForSwapAmount, other.reservedForSwapAmount)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = brokerId.hashCode()
        result = 31 * result + accountId.hashCode()
        result = 31 * result + clientId.hashCode()
        result = 31 * result + assetId.hashCode()
        result = 31 * result + amount.stripTrailingZeros().hashCode()
        result = 31 * result + reservedAmount.stripTrailingZeros().hashCode()
        result = 31 * result + reservedForSwapAmount.stripTrailingZeros().hashCode()
        return result
    }
}