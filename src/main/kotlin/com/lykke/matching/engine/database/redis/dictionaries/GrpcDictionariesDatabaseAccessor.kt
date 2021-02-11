package com.lykke.matching.engine.database.redis.dictionaries

import com.google.protobuf.Empty
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.AssetPair
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.utils.logging.ThrottlingLogger
import com.matching.engine.database.grpc.AssetPairsGrpc
import com.matching.engine.database.grpc.AssetsGrpc
import com.matching.engine.database.grpc.GrpcDictionaries
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.*

class GrpcDictionariesDatabaseAccessor(private val grpcConnectionString: String) : DictionariesDatabaseAccessor {
    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GrpcDictionariesDatabaseAccessor::class.java.name)
    }

    private var channel: ManagedChannel = ManagedChannelBuilder.forTarget(grpcConnectionString).usePlaintext().build()
    private var assetGrpcStub = AssetsGrpc.newBlockingStub(channel)
    private var assetPairGrpcStub = AssetPairsGrpc.newBlockingStub(channel)

    override fun loadAssets(): MutableMap<String, Asset> {
        val result = HashMap<String, Asset>()
        try {
            val response = assetGrpcStub.getAll(Empty.getDefaultInstance())
            response.assetsList.forEach { asset ->
                val convertedAsset = convertToAsset(asset)
                result[convertedAsset.symbol] = convertedAsset
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to load assets: ${e.message}", e)
            channel.shutdown()
            initConnection()
        }
        return result
    }

    override fun loadAsset(assetId: String): Asset? {
        try {
            val response = assetGrpcStub.getBySymbol(
                GrpcDictionaries.GetAssetBySymbolRequest.newBuilder().setBrokerId("").setSymbol(assetId).build()
            )
            return if (response != null) {
                convertToAsset(response.asset)
            } else {
                null
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to load asset $assetId: ${e.message}", e)
            channel.shutdown()
            initConnection()
        }
        return null
    }

    private fun convertToAsset(asset: GrpcDictionaries.Asset): Asset {
        return Asset(
            asset.brokerId,
            asset.symbol,
            asset.accuracy
        )
    }

    override fun loadAssetPairs(): Map<String, AssetPair> {
        val result = HashMap<String, AssetPair>()
        try {
            val response = assetPairGrpcStub.getAll(Empty.getDefaultInstance())
            response.assetPairsList.forEach { assetPair ->
                val convertedAssetPair = convertToAssetPair(assetPair)
                result[convertedAssetPair.symbol] = convertedAssetPair
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to load asset pairs: ${e.message}", e)
            channel.shutdown()
            initConnection()
        }
        return result
    }

    override fun loadAssetPair(assetPairId: String): AssetPair? {
        try {
            val response = assetPairGrpcStub.getBySymbol(
                GrpcDictionaries.GetAssetPairBySymbolRequest.newBuilder().setBrokerId("").setSymbol(assetPairId)
                    .build()
            )
            return if (response != null) {
                convertToAssetPair(response.assetPair)
            } else {
                null
            }
        } catch (e: Exception) {
            LOGGER.error("Unable to load asset pair $assetPairId: ${e.message}", e)
            channel.shutdown()
            initConnection()
        }
        return null
    }

    private fun convertToAssetPair(assetPair: GrpcDictionaries.AssetPair): AssetPair {
        return AssetPair(
            assetPair.brokerId,
            assetPair.symbol,
            assetPair.baseAsset,
            assetPair.quotingAsset,
            assetPair.accuracy,
            assetPair.minVolume.toBigDecimal(),
            assetPair.maxVolume.toBigDecimal(),
            assetPair.maxOppositeVolume.toBigDecimal(),
            assetPair.marketOrderPriceThreshold.toBigDecimal()
        )
    }

    @Synchronized
    private fun initConnection() {
        channel = ManagedChannelBuilder.forTarget(grpcConnectionString).usePlaintext().build()
        assetGrpcStub = AssetsGrpc.newBlockingStub(channel)
        assetPairGrpcStub = AssetPairsGrpc.newBlockingStub(channel)
    }
}