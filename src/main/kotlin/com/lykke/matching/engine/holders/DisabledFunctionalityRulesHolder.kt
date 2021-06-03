package com.lykke.matching.engine.holders

import com.google.gson.Gson
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.cache.ApplicationGroupDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingCreateOrUpdateEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingDeleteEvent
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class DisabledFunctionalityRulesHolder(
    val applicationSettingsCache: ApplicationSettingsCache,
    val assetsPairsHolder: AssetsPairsHolder
) {

    @Volatile
    private var disabledFunctionalityData = DisabledFunctionalityData()

    @Autowired
    private lateinit var gson: Gson

    fun isTradeDisabled(assetPair: AssetPair?): Boolean {
        val disabledFunctionalityDataToCheck = disabledFunctionalityData

        if (assetPair == null) {
            return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.TRADE)
        }

        return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.TRADE)
                || disabledFunctionalityDataToCheck.disabledAssetPairIds.contains(assetPair.symbol)
    }

    fun isCashInDisabled(asset: Asset?): Boolean {
        val disabledFunctionalityDataToCheck = disabledFunctionalityData

        if (asset == null) {
            return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_IN)
        }

        return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_IN)
                || (disabledFunctionalityDataToCheck.disabledOperationsByAsset[asset.symbol]?.contains(OperationType.CASH_IN)
            ?: false)
                || disabledFunctionalityDataToCheck.disabledAssetIds.contains(asset.symbol)

    }

    fun isCashOutDisabled(asset: Asset?): Boolean {
        val disabledFunctionalityDataToCheck = disabledFunctionalityData

        if (asset == null) {
            return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_OUT)
        }

        return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_OUT)
                || (disabledFunctionalityDataToCheck.disabledOperationsByAsset[asset.symbol]?.contains(OperationType.CASH_OUT)
            ?: false)
                || disabledFunctionalityDataToCheck.disabledAssetIds.contains(asset.symbol)
    }

    fun isCashTransferDisabled(asset: Asset?): Boolean {
        val disabledFunctionalityDataToCheck = disabledFunctionalityData

        if (asset == null) {
            return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_TRANSFER)
        }

        return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_TRANSFER)
                || (disabledFunctionalityDataToCheck.disabledOperationsByAsset[asset.symbol]?.contains(OperationType.CASH_TRANSFER)
            ?: false)
                || disabledFunctionalityDataToCheck.disabledAssetIds.contains(asset.symbol)
    }

    fun isCashSwapDisabled(asset: Asset?): Boolean {
        val disabledFunctionalityDataToCheck = disabledFunctionalityData

        if (asset == null) {
            return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_SWAP)
        }

        return disabledFunctionalityDataToCheck.disabledOperations.contains(OperationType.CASH_SWAP)
                || (disabledFunctionalityDataToCheck.disabledOperationsByAsset[asset.symbol]?.contains(OperationType.CASH_SWAP)
            ?: false)
                || disabledFunctionalityDataToCheck.disabledAssetIds.contains(asset.symbol)
    }

    @PostConstruct
    private fun init() {
        val disabledAssetPairIds = HashSet<String>()
        val disabledOperations = HashSet<OperationType>()
        val disabledAssetIds = HashSet<String>()
        val disabledOperationsByAsset = HashMap<String, MutableSet<OperationType>>()

        applicationSettingsCache.getSettingsGroup(
            AvailableSettingGroup.DISABLED_FUNCTIONALITY_RULES,
            true
        )?.settings?.forEach {
            val disabledRule = gson.fromJson(it.value, DisabledFunctionalityRule::class.java)

            if (disabledRule.assetPairId != null) {
                disabledAssetPairIds.add(disabledRule.assetPairId)
            }

            if (disabledRule.assetId != null) {
                if (disabledRule.operationType == OperationType.TRADE) {
                    disabledAssetPairIds.addAll(
                        assetsPairsHolder.getAssetPairsByAssetId(disabledRule.assetId).map { it.symbol })
                    return@forEach
                }

                if (disabledRule.operationType == null) {
                    disabledAssetPairIds.addAll(
                        assetsPairsHolder.getAssetPairsByAssetId(disabledRule.assetId).map { it.symbol })
                    disabledAssetIds.add(disabledRule.assetId)
                    return@forEach
                } else {
                    val disabledOperationsForAsset =
                        disabledOperationsByAsset.getOrPut(disabledRule.assetId) { HashSet() }
                    disabledOperationsForAsset.add(disabledRule.operationType)
                    return@forEach
                }
            }

            if (disabledRule.operationType != null && disabledRule.assetPairId == null) {
                disabledOperations.add(disabledRule.operationType)
            }
        }

        disabledFunctionalityData = DisabledFunctionalityData(
            disabledAssetPairIds = disabledAssetPairIds,
            disabledOperations = disabledOperations,
            disabledAssetIds = disabledAssetIds,
            disabledOperationsByAsset = disabledOperationsByAsset
        )

    }

    @EventListener
    fun onSettingCreateOrUpdate(applicationSettingUpdateEvent: ApplicationSettingCreateOrUpdateEvent) {
        init()
    }

    @EventListener
    fun onSettingRemove(applicationSettingDeleteEvent: ApplicationSettingDeleteEvent) {
        init()
    }

    @EventListener
    fun onSettingGroupRemove(applicationGroupDeleteEvent: ApplicationGroupDeleteEvent) {
        init()
    }

    private fun clear() {
        disabledFunctionalityData = DisabledFunctionalityData()
    }
}