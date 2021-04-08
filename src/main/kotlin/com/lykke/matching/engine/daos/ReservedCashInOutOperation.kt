package com.lykke.matching.engine.daos

import com.lykke.matching.engine.utils.NumberUtils
import java.math.BigDecimal
import java.util.*

data class ReservedCashInOutOperation(
    val matchingEngineOperationId: String,
    val externalId: String?,
    val brokerId: String,
    val accountId: String,
    val walletId: String,
    val asset: Asset?,
    val dateTime: Date,
    val reservedAmount: BigDecimal,
    val reservedSwapAmount: BigDecimal
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ReservedCashInOutOperation

        if (matchingEngineOperationId != other.matchingEngineOperationId) return false
        if (externalId != other.externalId) return false
        if (brokerId != other.brokerId) return false
        if (accountId != other.accountId) return false
        if (walletId != other.walletId) return false
        if (asset != other.asset) return false
        if (dateTime != other.dateTime) return false
        if (!NumberUtils.equalsIgnoreScale(reservedAmount, other.reservedAmount)) return false
        if (!NumberUtils.equalsIgnoreScale(reservedSwapAmount, other.reservedSwapAmount)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = matchingEngineOperationId.hashCode()
        result = 31 * result + (externalId?.hashCode() ?: 0)
        result = 31 * result + brokerId.hashCode()
        result = 31 * result + accountId.hashCode()
        result = 31 * result + walletId.hashCode()
        result = 31 * result + (asset?.hashCode() ?: 0)
        result = 31 * result + dateTime.hashCode()
        result = 31 * result + reservedAmount.stripTrailingZeros().hashCode()
        result = 31 * result + reservedSwapAmount.stripTrailingZeros().hashCode()
        return result
    }
}