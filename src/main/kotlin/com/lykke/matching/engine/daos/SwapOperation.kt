package com.lykke.matching.engine.daos

import java.math.BigDecimal
import java.util.*

data class SwapOperation(
    val matchingEngineOperationId: String,
    val externalId: String,
    val brokerId: String,
    val accountId1: String,
    val walletId1: String,
    val asset1: Asset?,
    val volume1: BigDecimal,
    val accountId2: String,
    val walletId2: String,
    val asset2: Asset?,
    val volume2: BigDecimal,
    val dateTime: Date,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SwapOperation

        if (matchingEngineOperationId != other.matchingEngineOperationId) return false
        if (externalId != other.externalId) return false
        if (brokerId != other.brokerId) return false
        if (accountId1 != other.accountId1) return false
        if (walletId1 != other.walletId1) return false
        if (asset1 != other.asset1) return false
        if (volume1 != other.volume1) return false
        if (accountId2 != other.accountId2) return false
        if (walletId2 != other.walletId2) return false
        if (asset2 != other.asset2) return false
        if (volume2 != other.volume2) return false
        if (dateTime != other.dateTime) return false

        return true
    }

    override fun hashCode(): Int {
        var result = matchingEngineOperationId.hashCode()
        result = 31 * result + externalId.hashCode()
        result = 31 * result + brokerId.hashCode()
        result = 31 * result + accountId1.hashCode()
        result = 31 * result + walletId1.hashCode()
        result = 31 * result + asset1.hashCode()
        result = 31 * result + volume1.hashCode()
        result = 31 * result + accountId2.hashCode()
        result = 31 * result + walletId2.hashCode()
        result = 31 * result + asset2.hashCode()
        result = 31 * result + volume2.hashCode()
        result = 31 * result + dateTime.hashCode()
        return result
    }
}