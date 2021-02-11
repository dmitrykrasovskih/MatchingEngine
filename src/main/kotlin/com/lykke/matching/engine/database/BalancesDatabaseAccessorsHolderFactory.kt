package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.azure.AzureWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class BalancesDatabaseAccessorsHolderFactory: FactoryBean<BalancesDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var redisWalletDatabaseAccessor: Optional<RedisWalletDatabaseAccessor>

    override fun getObjectType(): Class<*> {
        return BalancesDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): BalancesDatabaseAccessorsHolder {
        val primaryAccessor: WalletDatabaseAccessor
        val secondaryAccessor: WalletDatabaseAccessor?

        when (config.matchingEngine.storage) {
            Storage.Azure -> {
                primaryAccessor = AzureWalletDatabaseAccessor(config.matchingEngine.db.balancesInfoConnString,
                        config.matchingEngine.db.accountsTableName ?: AzureWalletDatabaseAccessor.DEFAULT_BALANCES_TABLE_NAME)

                secondaryAccessor = null
            }
            Storage.RedisWithoutOrders,
            Storage.Redis -> {
                primaryAccessor = redisWalletDatabaseAccessor.get()

                secondaryAccessor = if (config.matchingEngine.writeBalancesToSecondaryDb)
                    AzureWalletDatabaseAccessor(config.matchingEngine.db.balancesInfoConnString, config.matchingEngine.db.newAccountsTableName!!)
                else null
            }
        }
        return BalancesDatabaseAccessorsHolder(primaryAccessor, secondaryAccessor)
    }
}