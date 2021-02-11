package com.lykke.matching.engine.daos

import java.math.BigDecimal

class AssetPair(
    val brokerId: String,
    val symbol: String,
    val baseAssetId: String,
    val quotingAssetId: String,
    val accuracy: Int,
    val minVolume: BigDecimal = BigDecimal.ZERO,
    val maxVolume: BigDecimal = BigDecimal.ZERO,
    val maxValue: BigDecimal = BigDecimal.ZERO,
    val marketOrderPriceDeviationThreshold: BigDecimal = BigDecimal.ZERO
) {
    override fun toString(): String {
        return "AssetPair(" +
                "brokerId='$brokerId', " +
                "symbol='$symbol', " +
                "baseAssetId='$baseAssetId', " +
                "quotingAssetId='$quotingAssetId', " +
                "accuracy=$accuracy, " +
                "minVolume=$minVolume, " +
                "maxVolume=$maxVolume, " +
                "maxValue=$maxValue, " +
                "marketOrderPriceDeviationThreshold=$marketOrderPriceDeviationThreshold"
    }
}