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
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.utils.logging.MetricsLogger
import org.apache.log4j.Logger
import java.math.BigDecimal
import java.util.*
import kotlin.collections.HashMap

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
                getChangedAssetBalance(operation.clientId, operation.assetId)
            }

            val asset = assetsHolder.getAsset(operation.assetId)
            changedAssetBalance.balance =
                NumberUtils.setScaleRoundHalfUp(changedAssetBalance.balance + operation.amount, asset.accuracy)
            changedAssetBalance.reserved = if (!applicationSettingsHolder.isTrustedClient(operation.clientId))
                NumberUtils.setScaleRoundHalfUp(changedAssetBalance.reserved + operation.reservedAmount, asset.accuracy)
            else
                changedAssetBalance.reserved
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
                changedAssetBalance.reserved
            )
        }
        update.newBalance = changedAssetBalance.balance
        update.newReserved = changedAssetBalance.reserved
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

    fun sendNotification(id: String, type: String, messageId: String) {
        if (clientBalanceUpdatesByClientIdAndAssetId.isNotEmpty()) {
            balancesHolder.sendBalanceUpdate(
                BalanceUpdate(
                    id,
                    type,
                    Date(),
                    clientBalanceUpdatesByClientIdAndAssetId.values.toList(),
                    messageId
                )
            )
        }
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
        return if (balance.reserved > BigDecimal.ZERO)
            balance.balance - balance.reserved
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

    override fun getReservedBalance(
        brokerId: String,
        accountId: String,
        clientId: String,
        assetId: String
    ): BigDecimal {
        return getChangedCopyOrOriginalAssetBalance(brokerId, accountId, clientId, assetId).reserved
    }

    private fun isTrustedClientReservedBalanceOperation(operation: WalletOperation): Boolean {
        return NumberUtils.equalsIgnoreScale(
            BigDecimal.ZERO,
            operation.amount
        ) && applicationSettingsHolder.isTrustedClient(operation.clientId)
    }

    private fun getChangedAssetBalance(clientId: String, assetId: String): ChangedAssetBalance {
        val walletAssetBalance = getCurrentTransactionWalletAssetBalance(clientId, assetId)
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

    private fun getCurrentTransactionWalletAssetBalance(clientId: String, assetId: String): WalletAssetBalance {
        return currentTransactionBalancesHolder.getWalletAssetBalance(clientId, assetId)
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
    var balance = originBalance
    var reserved = originReserved
    var version = assetBalance.version

    fun isChanged(): Boolean {
        return !NumberUtils.equalsIgnoreScale(originBalance, balance) ||
                !NumberUtils.equalsIgnoreScale(originReserved, reserved)
    }

    fun apply(): Wallet {
        wallet.setBalance(assetId, balance)
        wallet.setReservedBalance(assetId, reserved)
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
        assetBalance.balance,
        assetBalance.reserved
    )

@Throws(BalanceException::class)
fun validateBalanceChange(
    clientId: String,
    assetId: String,
    oldBalance: BigDecimal,
    oldReserved: BigDecimal,
    newBalance: BigDecimal,
    newReserved: BigDecimal
) {
    val balanceInfo =
        "Invalid balance (client=$clientId, asset=$assetId, oldBalance=$oldBalance, oldReserved=$oldReserved, newBalance=$newBalance, newReserved=$newReserved)"

    // Balance can become negative earlier due to transfer operation with overdraftLimit > 0.
    // In this case need to check only difference of reserved & main balance.
    // It shouldn't be greater than previous one.
    if (newBalance < BigDecimal.ZERO && !(oldBalance < BigDecimal.ZERO && (oldBalance >= newBalance || oldReserved + newBalance >= newReserved + oldBalance))) {
        throw BalanceException(balanceInfo)
    }
    if (newReserved < BigDecimal.ZERO && oldReserved > newReserved) {
        throw BalanceException(balanceInfo)
    }

    // equals newBalance < newReserved && oldReserved - oldBalance < newReserved - newBalance
    if (newBalance < newReserved && oldReserved + newBalance < newReserved + oldBalance) {
        throw BalanceException(balanceInfo)
    }
}