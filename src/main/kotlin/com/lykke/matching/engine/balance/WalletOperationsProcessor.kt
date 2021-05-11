package com.lykke.matching.engine.balance

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.entity.BalancesData
import com.lykke.matching.engine.database.common.entity.OrderBooksPersistenceData
import com.lykke.matching.engine.database.common.entity.PersistenceData
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.order.transaction.CurrentTransactionBalancesHolder
import com.lykke.matching.engine.order.transaction.WalletAssetBalance
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.math.BigDecimal

class WalletOperationsProcessor(
    private val balancesHolder: BalancesHolder,
    private val currentTransactionBalancesHolder: CurrentTransactionBalancesHolder,
    private val applicationSettingsHolder: ApplicationSettingsHolder,
    private val persistenceManager: PersistenceManager,
    private val assetsHolder: AssetsHolder,
    private val logger: Logger?
) : BalancesGetter {

    companion object {
        private val LOGGER = Logger.getLogger(WalletOperationsProcessor::class.java.name)
        private val METRICS_LOGGER = MetricsLogger.getLogger()
    }

    private val clientBalanceUpdatesByClientIdAndAssetId = HashMap<String, ClientBalanceUpdate>()

    fun preProcess(operations: Collection<WalletOperation>, forceApply: Boolean = false): WalletOperationsProcessor {
        if (operations.isEmpty()) {
            return this
        }
        val changedAssetBalances = HashMap<String, ChangedAssetBalance>()
        operations.forEach { operation ->
            if (isTrustedClientReservedBalanceOperation(operation)) {
                return@forEach
            }
            val changedAssetBalance = changedAssetBalances.getOrPut(generateKey(operation)) {
                getChangedAssetBalance(operation.brokerId, operation.accountId, operation.clientId, operation.assetId)
            }

            val asset = assetsHolder.getAsset(operation.assetId)
            changedAssetBalance.balance =
                NumberUtils.setScaleRoundHalfUp(changedAssetBalance.balance + operation.amount, asset.accuracy)
            changedAssetBalance.reservedForOrders = if (!applicationSettingsHolder.isTrustedClient(operation.clientId))
                NumberUtils.setScaleRoundHalfUp(
                    changedAssetBalance.reservedForOrders + operation.reservedAmount,
                    asset.accuracy
                )
            else
                changedAssetBalance.reservedForOrders
            changedAssetBalance.reservedForSwap = NumberUtils.setScaleRoundHalfUp(
                changedAssetBalance.reservedForSwap + operation.reservedForSwapAmount,
                asset.accuracy
            )
        }

        try {
            changedAssetBalances.values.forEach { validateBalanceChange(it) }
        } catch (e: BalanceException) {
            if (!forceApply) {
                throw e
            }
            val message = "Force applying of invalid balance: ${e.message}"
            (logger ?: LOGGER).error(message)
            METRICS_LOGGER.logError(message, e)
        }

        changedAssetBalances.forEach { processChangedAssetBalance(it.value) }
        return this
    }

    private fun processChangedAssetBalance(changedAssetBalance: ChangedAssetBalance) {
        if (!changedAssetBalance.isChanged()) {
            return
        }
        changedAssetBalance.apply()
        generateEventData(changedAssetBalance)
    }

    private fun generateEventData(changedAssetBalance: ChangedAssetBalance) {
        val key = generateKey(changedAssetBalance)
        val update = clientBalanceUpdatesByClientIdAndAssetId.getOrPut(key) {
            ClientBalanceUpdate(
                changedAssetBalance.brokerId,
                changedAssetBalance.accountId,
                changedAssetBalance.walletId,
                changedAssetBalance.assetId,
                changedAssetBalance.version,
                changedAssetBalance.originBalance,
                changedAssetBalance.balance,
                changedAssetBalance.originReserved,
                changedAssetBalance.reservedForOrders
            )
        }
        update.newBalance = changedAssetBalance.balance
        update.newReserved = changedAssetBalance.reservedForOrders.add(changedAssetBalance.reservedForSwap)
        if (isBalanceUpdateNotificationNotNeeded(update)) {
            clientBalanceUpdatesByClientIdAndAssetId.remove(key)
        }
    }

    private fun isBalanceUpdateNotificationNotNeeded(clientBalanceUpdate: ClientBalanceUpdate): Boolean {
        return NumberUtils.equalsIgnoreScale(clientBalanceUpdate.oldBalance, clientBalanceUpdate.newBalance) &&
                NumberUtils.equalsIgnoreScale(clientBalanceUpdate.oldReserved, clientBalanceUpdate.newReserved)
    }

    fun apply(): WalletOperationsProcessor {
        currentTransactionBalancesHolder.apply()
        return this
    }

    fun persistenceData(): BalancesData {
        return currentTransactionBalancesHolder.persistenceData()
    }

    fun persistBalances(
        processedMessage: ProcessedMessage?,
        orderBooksData: OrderBooksPersistenceData?,
        stopOrderBooksData: OrderBooksPersistenceData?,
        messageSequenceNumber: Long?
    ): Boolean {
        return persistenceManager.persist(
            PersistenceData(
                persistenceData(),
                processedMessage,
                orderBooksData,
                stopOrderBooksData,
                messageSequenceNumber
            )
        )
    }

    fun getClientBalanceUpdates(): List<ClientBalanceUpdate> {
        return clientBalanceUpdatesByClientIdAndAssetId.values.toList()
    }

    override fun getAvailableBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): BigDecimal {
        val balance = getChangedCopyOrOriginalAssetBalance(brokerId, accountId, clientId, assetId)
        val totalReserved = balance.getTotalReserved()
        return if (totalReserved > BigDecimal.ZERO)
            balance.balance - totalReserved
        else
            balance.balance
    }

    override fun getAvailableReservedBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): BigDecimal {
        val balance = getChangedCopyOrOriginalAssetBalance(brokerId, accountId, clientId, assetId)
        return if (balance.reserved > BigDecimal.ZERO && balance.reserved < balance.balance)
            balance.reserved
        else
            balance.balance
    }

    override fun getReservedForOrdersBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): BigDecimal {
        return getChangedCopyOrOriginalAssetBalance(brokerId, accountId, clientId, assetId).reserved
    }

    override fun getReservedTotalBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): BigDecimal {
        return getChangedCopyOrOriginalAssetBalance(brokerId, accountId, clientId, assetId).getTotalReserved()
    }

    private fun isTrustedClientReservedBalanceOperation(operation: WalletOperation): Boolean {
        return NumberUtils.equalsIgnoreScale(
            BigDecimal.ZERO,
            operation.amount
        ) && applicationSettingsHolder.isTrustedClient(operation.clientId)
    }

    private fun getChangedAssetBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): ChangedAssetBalance {
        val walletAssetBalance = getCurrentTransactionWalletAssetBalance(brokerId, accountId, clientId, assetId)
        return ChangedAssetBalance(walletAssetBalance.wallet, walletAssetBalance.assetBalance)
    }

    private fun getChangedCopyOrOriginalAssetBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): AssetBalance {
        return currentTransactionBalancesHolder.getChangedCopyOrOriginalAssetBalance(
            brokerId,
            accountId,
            clientId,
            assetId
        )
    }

    private fun getCurrentTransactionWalletAssetBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): WalletAssetBalance {
        return currentTransactionBalancesHolder.getWalletAssetBalance(brokerId, accountId, clientId, assetId)
    }
}

private class ChangedAssetBalance(
    private val wallet: Wallet,
    assetBalance: AssetBalance
) {

    val brokerId = wallet.brokerId
    val accountId = wallet.accountId
    val walletId = wallet.clientId
    val assetId = assetBalance.asset
    val originBalance = assetBalance.balance
    val originReserved = assetBalance.reserved
    val originReservedForSwap = assetBalance.reservedForSwap
    var balance = originBalance
    var reservedForOrders = originReserved
    var reservedForSwap = originReservedForSwap
    var version = assetBalance.version

    fun isChanged(): Boolean {
        return !NumberUtils.equalsIgnoreScale(originBalance, balance) ||
                !NumberUtils.equalsIgnoreScale(originReserved, reservedForOrders) ||
                !NumberUtils.equalsIgnoreScale(originReservedForSwap, reservedForSwap)
    }

    fun apply(): Wallet {
        wallet.setBalance(assetId, balance)
        wallet.setReservedForOrdersBalance(assetId, reservedForOrders)
        wallet.setReservedForSwapBalance(assetId, reservedForSwap)
        wallet.increaseWalletVersion(assetId)
        version++
        return wallet
    }
}

private fun generateKey(operation: WalletOperation) = generateKey(operation.clientId, operation.assetId)

private fun generateKey(assetBalance: ChangedAssetBalance) = generateKey(assetBalance.walletId, assetBalance.assetId)

private fun generateKey(clientId: String, assetId: String) = "${clientId}_$assetId"

@Throws(BalanceException::class)
private fun validateBalanceChange(assetBalance: ChangedAssetBalance) =
    validateBalanceChange(
        assetBalance.walletId,
        assetBalance.assetId,
        assetBalance.originBalance,
        assetBalance.originReserved,
        assetBalance.originReservedForSwap,
        assetBalance.balance,
        assetBalance.reservedForOrders,
        assetBalance.reservedForSwap
    )

@Throws(BalanceException::class)
fun validateBalanceChange(
    clientId: String,
    assetId: String,
    oldBalance: BigDecimal,
    oldReserved: BigDecimal,
    oldReservedForSwap: BigDecimal,
    newBalance: BigDecimal,
    newReserved: BigDecimal,
    newReservedForSwap: BigDecimal
) {
    val balanceInfo =
        "Invalid balance (client=$clientId, asset=$assetId, oldBalance=$oldBalance, oldReserved=$oldReserved, newBalance=$newBalance, newReserved=$newReserved)"

    // Balance can become negative earlier due to transfer operation with overdraftLimit > 0.
    // In this case need to check only difference of reserved & main balance.
    // It shouldn't be greater than previous one.
    if (newBalance < BigDecimal.ZERO && !(oldBalance < BigDecimal.ZERO && (oldBalance >= newBalance || oldReserved + oldReservedForSwap + newBalance >= newReserved + newReservedForSwap + oldBalance))) {
        throw BalanceException(balanceInfo)
    }
    if (newReserved < BigDecimal.ZERO && oldReserved > newReserved) {
        throw BalanceException(balanceInfo)
    }
    if (newReservedForSwap < BigDecimal.ZERO && oldReservedForSwap > newReservedForSwap) {
        throw BalanceException(balanceInfo)
    }

    // equals newBalance < newReserved && oldReserved - oldBalance < newReserved - newBalance
    if (newBalance < newReserved + newReservedForSwap && oldReserved + oldReservedForSwap + newBalance < newReserved + newReservedForSwap + oldBalance) {
        throw BalanceException(balanceInfo)
    }
}