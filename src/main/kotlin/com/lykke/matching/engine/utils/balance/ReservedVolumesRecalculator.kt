package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.balance.ClientOrdersReservedVolume
import com.lykke.matching.engine.daos.balance.ReservedVolumeCorrection
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.ReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.builders.EventFactory
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
@Order(3)
class ReservedVolumesRecalculator @Autowired constructor(
    private val orderBookDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
    private val stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
    private val reservedVolumesDatabaseAccessor: ReservedVolumesDatabaseAccessor,
    private val assetsHolder: AssetsHolder,
    private val assetsPairsHolder: AssetsPairsHolder,
    private val balancesHolder: BalancesHolder,
    private val applicationSettingsHolder: ApplicationSettingsHolder,
    @Value("#{Config.matchingEngine.correctReservedVolumes}") private val correctReservedVolumes: Boolean,
    private val messageSequenceNumberHolder: MessageSequenceNumberHolder,
    private val messageSender: MessageSender
) : ApplicationRunner {
    override fun run(args: ApplicationArguments?) {
        correctReservedVolumesIfNeed()
    }

    companion object {
        private val LOGGER = Logger.getLogger(ReservedVolumesRecalculator::class.java.name)
    }

    fun correctReservedVolumesIfNeed() {
        if (!correctReservedVolumes) {
            return
        }

        LOGGER.info("Starting order books analyze")
        recalculate()
    }

    fun recalculate() {

        val orders = orderBookDatabaseAccessorHolder.primaryAccessor.loadLimitOrders()
        val stopOrders = stopOrdersDatabaseAccessorsHolder.primaryAccessor.loadStopLimitOrders()

        val reservedBalances = HashMap<String, MutableMap<String, ClientOrdersReservedVolume>>()
        var count = 1

        val handleOrder: (order: LimitOrder, isStopOrder: Boolean) -> Unit = { order, isStopOrder ->
            if (!applicationSettingsHolder.isTrustedClient(order.clientId)) {
                try {
                    if (isStopOrder) {
                        LOGGER.info("${count++} Client:${order.clientId}, id: ${order.externalId}, asset:${order.assetPairId}, lowerLimitPrice:${order.lowerLimitPrice}, lowerPrice:${order.lowerPrice}, upperLimitPrice:${order.upperLimitPrice}, upperPrice:${order.upperPrice}, volume:${order.volume}, date:${order.registered}, status:${order.status}, reserved: ${order.reservedLimitVolume}}")
                    } else {
                        LOGGER.info("${count++} Client:${order.clientId}, id: ${order.externalId}, asset:${order.assetPairId}, price:${order.price}, volume:${order.volume}, date:${order.registered}, status:${order.status}, reserved: ${order.reservedLimitVolume}}")
                    }
                    val assetPair = assetsPairsHolder.getAssetPair(order.assetPairId)
                    val asset =
                        assetsHolder.getAsset(if (order.isBuySide()) assetPair.quotingAssetId else assetPair.baseAssetId)

                    val reservedVolume = if (order.reservedLimitVolume != null) {
                        order.reservedLimitVolume!!
                    } else {
                        val calculatedReservedVolume = if (order.isBuySide())
                            NumberUtils.setScale(
                                if (isStopOrder) order.volume * (order.upperPrice ?: order.lowerPrice)!!
                                else order.getAbsRemainingVolume() * order.price, asset.accuracy, false
                            )
                        else order.getAbsRemainingVolume()
                        LOGGER.info("Null reserved volume, recalculated: $calculatedReservedVolume")
                        calculatedReservedVolume
                    }

                    val clientAssets = reservedBalances.getOrPut(order.clientId) { HashMap() }
                    val balance = clientAssets.getOrPut(asset.symbol) { ClientOrdersReservedVolume() }
                    val newBalance = NumberUtils.setScaleRoundHalfUp(balance.volume + reservedVolume, asset.accuracy)
                    balance.volume = newBalance
                    balance.orderIds.add(order.externalId)
                } catch (e: Exception) {
                    val errorMessage = "Unable to handle order (id: ${order.externalId}): ${e.message}"
                    LOGGER.error(errorMessage)
                }
            }
        }

        orders.forEach { handleOrder(it, false) }
        stopOrders.forEach { handleOrder(it, true) }

        LOGGER.info("---------------------------------------------------------------------------------------------------")

        reservedBalances.forEach { client ->
            LOGGER.info("${client.key} : ${client.value}")
        }

        LOGGER.info("---------------------------------------------------------------------------------------------------")

        val corrections = LinkedList<ReservedVolumeCorrection>()
        val updatedWallets = mutableSetOf<Wallet>()
        val balanceUpdates = mutableListOf<ClientBalanceUpdate>()
        balancesHolder.wallets.forEach {
            val wallet = it.value
            val id = wallet.clientId
            wallet.balances.values.forEach { assetBalance ->
                val oldBalance = assetBalance.reserved
                val newBalance = reservedBalances[id]?.get(assetBalance.asset)
                if (newBalance != null && newBalance.volume > BigDecimal.ZERO) {
                    if (!NumberUtils.equalsIgnoreScale(oldBalance, newBalance.volume)) {
                        val correction = ReservedVolumeCorrection(
                            id,
                            assetBalance.asset,
                            newBalance.orderIds.joinToString(","),
                            oldBalance,
                            newBalance.volume
                        )
                        corrections.add(correction)
                        LOGGER.info("1 $id, ${assetBalance.asset} : Old $oldBalance New $newBalance")
                        wallet.setReservedForOrdersBalance(assetBalance.asset, newBalance.volume)
                        updatedWallets.add(wallet)
                        val balanceUpdate = ClientBalanceUpdate(
                            assetBalance.brokerId, assetBalance.accountId, id,
                            assetBalance.asset,
                            assetBalance.version,
                            assetBalance.balance,
                            assetBalance.balance,
                            oldBalance,
                            newBalance.volume
                        )
                        balanceUpdates.add(balanceUpdate)
                    }
                } else if (!NumberUtils.equalsIgnoreScale(oldBalance, BigDecimal.ZERO)) {
                    val orderIds = newBalance?.orderIds?.joinToString(",")
                    val correction = ReservedVolumeCorrection(
                        id,
                        assetBalance.asset,
                        orderIds,
                        oldBalance,
                        newBalance?.volume ?: BigDecimal.ZERO
                    )
                    corrections.add(correction)
                    LOGGER.info("2 $id, ${assetBalance.asset} : Old $oldBalance New ${newBalance ?: 0.0}")
                    wallet.setReservedForOrdersBalance(assetBalance.asset, BigDecimal.ZERO)
                    updatedWallets.add(wallet)
                    val balanceUpdate = ClientBalanceUpdate(
                        assetBalance.brokerId, assetBalance.accountId, id,
                        assetBalance.asset,
                        assetBalance.version,
                        assetBalance.balance,
                        assetBalance.balance,
                        oldBalance,
                        BigDecimal.ZERO
                    )
                    balanceUpdates.add(balanceUpdate)
                }
            }
        }
        if (updatedWallets.isNotEmpty()) {
            val now = Date()
            val operationId = UUID.randomUUID().toString()
            LOGGER.info("Starting balances update, operationId: $operationId")

            var sequenceNumber: Long? = null
            val cashInOutEvents = mutableListOf<Event<*>>()
            balanceUpdates.forEach { clientBalanceUpdate ->
                sequenceNumber = messageSequenceNumberHolder.getNewValue()
                val walletOperation = WalletOperation(
                    clientBalanceUpdate.brokerId, clientBalanceUpdate.accountId, clientBalanceUpdate.walletId,
                    clientBalanceUpdate.asset,
                    BigDecimal.ZERO,
                    clientBalanceUpdate.newReserved - clientBalanceUpdate.oldReserved
                )
                cashInOutEvents.add(
                    EventFactory.createCashInOutEvent(
                        clientBalanceUpdate.newReserved - clientBalanceUpdate.oldReserved,
                        sequenceNumber!!,
                        operationId,
                        operationId,
                        now,
                        MessageType.RESERVED_CASH_IN_OUT_OPERATION,
                        listOf(clientBalanceUpdate),
                        walletOperation,
                        emptyList()
                    )
                )
            }

            balancesHolder.insertOrUpdateWallets(updatedWallets, sequenceNumber)
            reservedVolumesDatabaseAccessor.addCorrectionsInfo(corrections)
            cashInOutEvents.forEach { messageSender.sendMessage(it) }

        }
        LOGGER.info("Reserved volume recalculation finished")
    }
}