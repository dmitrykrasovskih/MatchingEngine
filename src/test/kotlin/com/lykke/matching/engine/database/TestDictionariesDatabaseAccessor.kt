package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import java.util.*

class TestDictionariesDatabaseAccessor : DictionariesDatabaseAccessor {

    private val assetPairs = HashMap<String, AssetPair>()

    override fun loadAssetPairs(): HashMap<String, AssetPair> {
        return assetPairs
    }

    override fun loadAssetPair(assetPairId: String): AssetPair? {
        return assetPairs[assetPairId]
    }

    fun addAssetPair(pair: AssetPair) {
        assetPairs[pair.symbol] = pair
    }

    fun clear() {
        assets.clear()
        assetPairs.clear()
    }

    val assets = HashMap<String, Asset>()

    fun addAsset(asset: Asset) {
        assets[asset.symbol] = asset
    }

    override fun loadAsset(assetId: String): Asset? {
        return assets[assetId]
    }

    override fun loadAssets(): MutableMap<String, Asset> {
        return HashMap()
    }
}