package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.database.redis.accessor.impl.*
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
    fun cashOperationIdRedisConnection(): RedisConnection? {
        return redisConnectionFactory.getConnection("cashOperationIdRedisConnection")
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
        return RedisProcessedMessagesDatabaseAccessor(
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.processedMessageDatabase,
            getProcessedMessageTTL()
        )
    }


    @Bean
    fun redisWalletDatabaseAccessor(): RedisWalletDatabaseAccessor? {
        return RedisWalletDatabaseAccessor(
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.balanceDatabase
        )
    }

    @Bean
    fun redisEventDatabaseAccessor(): RedisEventDatabaseAccessor {
        return RedisEventDatabaseAccessor(
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.outgoingEventsDatabase,
            config.matchingEngine.redis.outgoingEventsTtl
        )
    }

    @Bean
    fun redisCashOperationIdDatabaseAccessor(): RedisCashOperationIdDatabaseAccessor? {
        return RedisCashOperationIdDatabaseAccessor(
            cashOperationIdRedisConnection()!!,
            config.matchingEngine.redis.processedCashMessageDatabase
        )
    }

    @Bean
    fun redisMessageSequenceNumberDatabaseAccessor(): RedisMessageSequenceNumberDatabaseAccessor? {
        return RedisMessageSequenceNumberDatabaseAccessor(
            "MessageSequenceNumber",
            initialLoadingRedisConnection()!!,
            config.matchingEngine.redis.sequenceNumberDatabase
        )
    }

    @Bean
    fun redisSentMessageSequenceNumberDatabaseAccessor(): RedisMessageSequenceNumberDatabaseAccessor? {
        return RedisMessageSequenceNumberDatabaseAccessor(
            "SentMessageSequenceNumber",
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