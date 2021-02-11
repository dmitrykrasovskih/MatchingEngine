package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair

interface DictionariesDatabaseAccessor {
    fun loadAsset(assetId: String): Asset?
    fun loadAssets(): MutableMap<String, Asset>

    fun loadAssetPair(assetPairId: String): AssetPair?
    fun loadAssetPairs(): Map<String, AssetPair>
}