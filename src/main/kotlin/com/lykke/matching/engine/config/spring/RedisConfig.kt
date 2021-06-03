package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.connection.RedisConnectionFactory
import com.lykke.matching.engine.database.redis.connection.impl.RedisReconnectionManager
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.TaskScheduler

@Configuration
class RedisConfig {

    @Autowired
    private lateinit var redisConnectionFactory: RedisConnectionFactory

    @Autowired
    private lateinit var config: Config

    //<editor-fold desc="Redis connections">
    @Bean
    fun pingRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("pingRedisConnection")
    }

    @Bean
    fun cashTransferOperationIdRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashTransferOperationIdRedisConnection")
    }

    @Bean
    fun cashInOutOperationIdRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashInOutOperationIdRedisConnection")
    }

    @Bean
    fun initialLoadingRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("initialLoadingRedisConnection")
    }

    @Bean
    fun persistenceRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("persistenceRedisConnection")
    }

    @Bean
    fun limitOrderCancelOperationPreprocessorRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("limitOrderCancelOperationPreprocessorRedisConnection")
    }

    @Bean
    fun cashInOutOperationPreprocessorRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashInOutOperationPreprocessorRedisConnection")
    }

    @Bean
    fun cashTransferOperationsPreprocessorRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashTransferOperationsPreprocessorRedisConnection")
    }

    @Bean
    fun cashSwapOperationsPreprocessorRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashSwapOperationsPreprocessorRedisConnection")
    }
    //</editor-fold>

    //<editor-fold desc="Redis database accessors">
    @Bean
    fun redisProcessedMessagesDatabaseAccessor(): RedisProcessedMessagesDatabaseAccessor? {
        if (config.matchingEngine.storage != Storage.Redis && config.matchingEngine.storage != Storage.RedisWithoutOrders) {
            return null
        }

        return RedisProcessedMessagesDatabaseAccessor(
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.processedMessageDatabase,
            getProcessedMessageTTL()
        )
    }


    @Bean
    fun redisWalletDatabaseAccessor(): RedisWalletDatabaseAccessor? {
        if (config.matchingEngine.storage != Storage.Redis && config.matchingEngine.storage != Storage.RedisWithoutOrders) {
            return null
        }

        return RedisWalletDatabaseAccessor(
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.balanceDatabase
        )
    }

    @Bean
    fun redisCashOperationIdDatabaseAccessor(): RedisCashOperationIdDatabaseAccessor? {
        if (config.matchingEngine.storage != Storage.Redis && config.matchingEngine.storage != Storage.RedisWithoutOrders) {
            return null
        }

        return RedisCashOperationIdDatabaseAccessor(
            cashInOutOperationIdRedisConnection()!!,
            cashTransferOperationIdRedisConnection()!!,
            config.matchingEngine.redis.processedCashMessageDatabase
        )
    }

    @Bean
    fun redisMessageSequenceNumberDatabaseAccessor(): RedisMessageSequenceNumberDatabaseAccessor? {
        if (config.matchingEngine.storage != Storage.Redis && config.matchingEngine.storage != Storage.RedisWithoutOrders) {
            return null
        }

        return RedisMessageSequenceNumberDatabaseAccessor(
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.sequenceNumberDatabase
        )
    }
    //</editor-fold>

    //<editor-fold desc="etc">
    @Bean
    fun redisReconnectionManager(
        taskScheduler: TaskScheduler,
        applicationEventPublisher: ApplicationEventPublisher,
        allRedisConnections: List<RedisConnection>,
        @Value("\${redis.health.check.interval}") updateInterval: Long,
        @Value("\${redis.health.check.reconnect.interval}") reconnectInterval: Long
    ): RedisReconnectionManager? {
        if (config.matchingEngine.storage != Storage.Redis && config.matchingEngine.storage != Storage.RedisWithoutOrders) {
            return null
        }

        return RedisReconnectionManager(
            config.matchingEngine, allRedisConnections, pingRedisConnection()!!,
            taskScheduler, applicationEventPublisher, updateInterval, reconnectInterval
        )
    }
    //</editor-fold>

    private fun getProcessedMessageTTL(): Int {
        return (config.matchingEngine.processedMessagesInterval / 500).toInt()
    }
}