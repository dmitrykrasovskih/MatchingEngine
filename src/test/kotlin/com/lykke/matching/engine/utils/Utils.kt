package com.lykke.matching.engine.utils

import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.daos.setting.Setting
import java.math.BigDecimal

fun getSetting(value: String, name: String = value) = Setting(name, value, true)

fun createAssetPair(
    brokerId: String,
    symbol: String,
    baseAssetId: String,
    quotingAssetId: String,
    accuracy: Int,
    minVolume: BigDecimal = BigDecimal.ZERO,
    maxVolume: BigDecimal = BigDecimal.valueOf(10000000),
    maxValue: BigDecimal = BigDecimal.valueOf(10000000),
    marketOrderPriceDeviationThreshold: BigDecimal = BigDecimal.valueOf(100)
) = AssetPair(
    brokerId,
    symbol,
    baseAssetId,
    quotingAssetId,
    accuracy,
    minVolume,
    maxVolume,
    maxValue,
    marketOrderPriceDeviationThreshold
)