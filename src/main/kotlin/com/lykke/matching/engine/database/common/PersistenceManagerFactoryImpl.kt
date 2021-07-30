package com.lykke.matching.engine.database.common

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.database.common.strategy.PersistOrdersDuringRedisTransactionStrategy
import com.lykke.matching.engine.database.redis.RedisPersistenceManager
import com.lykke.matching.engine.database.redis.accessor.impl.*
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
    private val redisProcessedMessagesDatabaseAccessor: RedisProcessedMessagesDatabaseAccessor,
    private val cashOperationIdDatabaseAccessor: RedisCashOperationIdDatabaseAccessor,
    private val redisMessageSequenceNumberDatabaseAccessor: RedisMessageSequenceNumberDatabaseAccessor,
    private val redisEventDatabaseAccessor: RedisEventDatabaseAccessor,
    private val config: Config,
    private val currentTransactionDataHolder: CurrentTransactionDataHolder,
    private val performanceStatsHolder: PerformanceStatsHolder,
    private val persistOrdersStrategy: PersistOrdersDuringRedisTransactionStrategy
) : PersistenceManagerFactory {

    override fun get(redisConnection: Optional<RedisConnection>): PersistenceManager {
        return createRedisPersistenceManager(redisConnection.get())
    }

    private fun createRedisPersistenceManager(redisConnection: RedisConnection): RedisPersistenceManager {
        return RedisPersistenceManager(
            balancesDatabaseAccessorsHolder.primaryAccessor as RedisWalletDatabaseAccessor,
            redisProcessedMessagesDatabaseAccessor,
            cashOperationIdDatabaseAccessor,
            persistOrdersStrategy,
            redisMessageSequenceNumberDatabaseAccessor,
            redisEventDatabaseAccessor,
            redisConnection,
            config,
            currentTransactionDataHolder,
            performanceStatsHolder
        )
    }
}