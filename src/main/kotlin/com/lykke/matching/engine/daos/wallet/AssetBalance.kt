package com.lykke.matching.engine.daos.wallet

import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.math.BigDecimal

class AssetBalance(
    val clientId: String,
    val asset: String,
    var balance: BigDecimal = BigDecimal.ZERO,
    var reserved: BigDecimal = BigDecimal.ZERO,
    @Version(2) var reservedForSwap: BigDecimal = BigDecimal.ZERO,
    @Version(1) var brokerId: String = "",
    @Version(1) var accountId: String = "",
    @Version(1) var version: Long = 0
) : Serializable {
    fun getTotalReserved(): BigDecimal = reserved + reservedForSwap
}