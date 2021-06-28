package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.ReadOnlyMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.common.strategy.PersistOrdersDuringRedisTransactionStrategy
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.CurrentTransactionDataHolder
import com.lykke.matching.engine.performance.PerformanceStatsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.stereotype.Component
import java.util.*

@Component
class PersistenceManagerFactoryImpl(
    private val balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
    private val redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>,
    private val cashOperationIdDatabaseAccessor: Optional<CashOperationIdDatabaseAccessor>,
    private val messageSequenceNumberDatabaseAccessor: Optional<ReadOnlyMessageSequenceNumberDatabaseAccessor>,
    private val config: Config,
    private val currentTransactionDataHolder: CurrentTransactionDataHolder,
    private val performanceStatsHolder: PerformanceStatsHolder,
    private val persistOrdersStrategy: Optional<PersistOrdersDuringRedisTransactionStrategy>
) : PersistenceManagerFactory {

    override fun get(redisConnection: Optional<RedisConnection>): PersistenceManager {
        return createRedisPersistenceManager(redisConnection.get())
    }

    private fun createRedisPersistenceManager(redisConnection: RedisConnection): RedisPersistenceManager {
        return RedisPersistenceManager(
            balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
            redisProcessedMessagesDatabaseAccessor.get(),
            cashOperationIdDatabaseAccessor.get() as RedisCashOperationIdDatabaseAccessor,
            persistOrdersStrategy.get(),
            messageSequenceNumberDatabaseAccessor.get() as RedisMessageSequenceNumberDatabaseAccessor,
            redisConnection,
            config,
            currentTransactionDataHolder,
            performanceStatsHolder
        )
    }
}