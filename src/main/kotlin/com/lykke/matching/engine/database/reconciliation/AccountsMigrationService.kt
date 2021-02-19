package com.lykke.matching.engine.database.reconciliation

import com.lykke.matching.engine.daos.wallet.Wallet
import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.exception.MatchingEngineException
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.utils.config.Config
import org.apache.logging.log4j.LogManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.util.*

@Component
@Order(1)
class AccountsMigrationService @Autowired constructor(
    private val balancesHolder: BalancesHolder,
    private val config: Config,
    private val redisWalletDatabaseAccessor: Optional<RedisWalletDatabaseAccessor>
) : ApplicationRunner {
    override fun run(args: ApplicationArguments?) {
        if (config.matchingEngine.walletsMigration) {
            migrateAccounts()
        }
    }

    companion object {
        private val LOGGER = LogManager.getLogger(AccountsMigrationService::class.java.name)
    }

    private val azureAccountsTableName = config.matchingEngine.db.accountsTableName
        ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME
    private val azureDatabaseAccessor =
        AzureWalletDatabaseAccessor(config.matchingEngine.db.balancesInfoConnString, azureAccountsTableName)

    fun migrateAccounts() {
        if (!config.matchingEngine.walletsMigration) {
            return
        }

        when (config.matchingEngine.storage) {
            Storage.Azure -> fromRedisToDb()
            Storage.RedisWithoutOrders,
            Storage.Redis -> fromDbToRedis()
        }
    }

    fun fromDbToRedis() {
        if (redisWalletDatabaseAccessor.get().loadWallets().isNotEmpty()) {
            throw AccountsMigrationException("Wallets already exist in redis ${config.matchingEngine.redis.host}.${config.matchingEngine.redis.port}")
        }

        val startTime = Date().time
        teeLog("Starting wallets migration from azure to redis; azure table: $azureAccountsTableName, redis: ${config.matchingEngine.redis.host}.${config.matchingEngine.redis.port}")
        val wallets = azureDatabaseAccessor.loadWallets()
        val loadTime = Date().time
        teeLog("Loaded ${wallets.size} wallets from azure (ms: ${loadTime - startTime})")
        balancesHolder.insertOrUpdateWallets(wallets.values.toList(), null)
        val saveTime = Date().time
        teeLog("Saved ${wallets.size} wallets to redis (ms: ${saveTime - loadTime})")

        compare()
    }

    fun fromRedisToDb() {
        val startTime = Date().time
        teeLog("Starting wallets migration from redis to azure; redis: ${config.matchingEngine.redis.host}.${config.matchingEngine.redis.port}, azure table: $azureAccountsTableName")
        val loadTime = Date().time
        val wallets = redisWalletDatabaseAccessor.get().loadWallets()
        if (wallets.isEmpty()) {
            throw AccountsMigrationException("There are no wallets in redis ${config.matchingEngine.redis.host}.${config.matchingEngine.redis.port}")
        }
        teeLog("Loaded ${wallets.size} wallets from redis (ms: ${loadTime - startTime})")
        balancesHolder.insertOrUpdateWallets(wallets.values.toList(), null)
        val saveTime = Date().time
        teeLog("Saved ${wallets.size} wallets to azure (ms: ${saveTime - loadTime})")

        compare()
    }

    private fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }

    /** Compares balances stored in redis & azure; logs comparison result  */
    private fun compare() {
        val azureWallets = azureDatabaseAccessor.loadWallets().filter { it.value.balances.isNotEmpty() }
        val redisWallets = redisWalletDatabaseAccessor.get().loadWallets()

        val onlyAzureClients = azureWallets.keys.filterNot { redisWallets.contains(it) }
        val onlyRedisClients = redisWallets.keys.filterNot { azureWallets.contains(it) }
        val commonClients = azureWallets.keys.filter { redisWallets.contains(it) }

        val differentWallets = LinkedList<String>()

        teeLog("Comparison result. Differences: ")
        teeLog("---------------------------------------------------------------------------------------------")
        commonClients.forEach {
            val azureWallet = azureWallets[it]
            val redisWallet = redisWallets[it]
            if (!compareBalances(azureWallet!!, redisWallet!!)) {
                differentWallets.add(it)
            }
        }
        teeLog("---------------------------------------------------------------------------------------------")

        teeLog("Total: ")
        teeLog("azure clients count: ${azureWallets.size}")
        teeLog("redis clients count: ${redisWallets.size}")
        teeLog("only azure clients (count: ${onlyAzureClients.size}): $onlyAzureClients")
        teeLog("only redis clients (count: ${onlyRedisClients.size}): $onlyRedisClients")
        teeLog("clients with different wallets (count: ${differentWallets.size}): $differentWallets")
    }

    private fun compareBalances(azureWallet: Wallet, redisWallet: Wallet): Boolean {
        if (azureWallet.clientId != redisWallet.clientId) {
            teeLog("different clients: ${azureWallet.clientId} & ${redisWallet.clientId}")
            return false
        }
        val clientId = azureWallet.clientId
        val azureBalances = azureWallet.balances
        val redisBalances = redisWallet.balances

        val onlyAzureAssets =
            azureBalances.keys.filterNot { redisBalances.keys.contains(it) || azureBalances[it]!!.balance.signum() == 0 }
        val onlyRedisAssets = redisBalances.keys.filterNot { azureBalances.keys.contains(it) }

        if (onlyAzureAssets.isNotEmpty() || onlyRedisAssets.isNotEmpty()) {
            teeLog("different asset sets: $onlyAzureAssets & $onlyRedisAssets, client: $clientId")
            return false
        }

        val commonAssets = redisBalances.keys.filter { azureBalances.keys.contains(it) }
        commonAssets.forEach {
            val azureBalance = azureBalances[it]
            val redisBalance = redisBalances[it]
            if (azureBalance!!.balance != redisBalance!!.balance) {
                teeLog("different balances: ${azureBalance.balance} & ${redisBalance.balance}, client: $clientId")
                return false
            }
            if (azureBalance.reserved != redisBalance.reserved) {
                teeLog("different reserved balances: ${azureBalance.reserved} & ${redisBalance.reserved}, client: $clientId")
                return false
            }
        }

        return true
    }
}

class AccountsMigrationException(message: String) : MatchingEngineException(message)
