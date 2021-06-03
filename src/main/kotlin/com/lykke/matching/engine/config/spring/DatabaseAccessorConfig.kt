package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.common.QueueConsumer
import com.lykke.matching.engine.common.SimpleApplicationEventPublisher
import com.lykke.matching.engine.common.impl.ApplicationEventPublisherImpl
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.azure.*
import com.lykke.matching.engine.database.common.PersistenceManagerFactory
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.file.FileProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.file.FileStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.listeners.OrderBookPersistListener
import com.lykke.matching.engine.database.listeners.StopOrderBookPersistListener
import com.lykke.matching.engine.database.listeners.WalletOperationsPersistListener
import com.lykke.matching.engine.database.reconciliation.events.AccountPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.OrderBookPersistEvent
import com.lykke.matching.engine.database.reconciliation.events.StopOrderBookPersistEvent
import com.lykke.matching.engine.database.redis.accessor.impl.RedisCashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisMessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisProcessedMessagesDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.database.redis.dictionaries.GrpcDictionariesDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.holders.StopOrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.util.*
import java.util.concurrent.BlockingQueue

@Configuration
class DatabaseAccessorConfig {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var persistenceManagerFactory: PersistenceManagerFactory

    //<editor-fold desc="Persistence managers">
    @Bean
    fun persistenceManager(persistenceRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(persistenceRedisConnection)
    }

    @Bean
    fun cashInOutOperationPreprocessorPersistenceManager(cashInOutOperationPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(cashInOutOperationPreprocessorRedisConnection)
    }


    @Bean
    fun limitOrderCancelOperationPreprocessorPersistenceManager(limitOrderCancelOperationPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(limitOrderCancelOperationPreprocessorRedisConnection)

    }

    @Bean
    fun cashTransferPreprocessorPersistenceManager(cashTransferOperationsPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(cashTransferOperationsPreprocessorRedisConnection)
    }

    @Bean
    fun cashSwapPreprocessorPersistenceManager(cashSwapOperationsPreprocessorRedisConnection: Optional<RedisConnection>): PersistenceManager {
        return persistenceManagerFactory.get(cashSwapOperationsPreprocessorRedisConnection)
    }
    //</editor-fold>


    //<editor-fold desc="Persist listeners">
    @Bean
    fun walletOperationsPersistListener(
        updatedWalletsQueue: BlockingQueue<AccountPersistEvent>,
        balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder
    ): QueueConsumer<AccountPersistEvent>? {
        return balancesDatabaseAccessorsHolder.secondaryAccessor?.let {
            WalletOperationsPersistListener(updatedWalletsQueue, balancesDatabaseAccessorsHolder.secondaryAccessor)
        }
    }

    @Bean
    fun orderBookPersistListener(
        updatedOrderBooksQueue: BlockingQueue<OrderBookPersistEvent>,
        ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder
    ): OrderBookPersistListener? {
        return ordersDatabaseAccessorsHolder.secondaryAccessor?.let {
            OrderBookPersistListener(
                updatedOrderBooksQueue,
                ordersDatabaseAccessorsHolder.secondaryAccessor
            )
        }
    }

    @Bean
    fun stopOrderBookPersistListener(
        updatedStopOrderBooksQueue: BlockingQueue<StopOrderBookPersistEvent>,
        stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder
    ): StopOrderBookPersistListener? {
        return stopOrdersDatabaseAccessorsHolder.secondaryAccessor?.let {
            StopOrderBookPersistListener(
                updatedStopOrderBooksQueue,
                stopOrdersDatabaseAccessorsHolder.secondaryAccessor
            )
        }
    }
    //</editor-fold>

    //<editor-fold desc="Multisource database accessors">
    @Bean
    fun readOnlyProcessedMessagesDatabaseAccessor(redisProcessedMessagesDatabaseAccessor: Optional<RedisProcessedMessagesDatabaseAccessor>): ReadOnlyProcessedMessagesDatabaseAccessor {
        return when (config.matchingEngine.storage) {
            Storage.Azure -> fileProcessedMessagesDatabaseAccessor()
            Storage.RedisWithoutOrders,
            Storage.Redis -> redisProcessedMessagesDatabaseAccessor.get()
        }
    }

    @Bean
    fun cashOperationIdDatabaseAccessor(redisCashOperationIdDatabaseAccessor: Optional<RedisCashOperationIdDatabaseAccessor>): CashOperationIdDatabaseAccessor? {
        return when (config.matchingEngine.storage) {
            Storage.Azure -> AzureCashOperationIdDatabaseAccessor()
            Storage.RedisWithoutOrders,
            Storage.Redis -> redisCashOperationIdDatabaseAccessor.get()
        }
    }

    @Bean
    fun messageSequenceNumberDatabaseAccessor(redisMessageSequenceNumberDatabaseAccessor: Optional<RedisMessageSequenceNumberDatabaseAccessor>): ReadOnlyMessageSequenceNumberDatabaseAccessor {
        return when (config.matchingEngine.storage) {
            Storage.Azure -> AzureMessageSequenceNumberDatabaseAccessor()
            Storage.RedisWithoutOrders,
            Storage.Redis -> redisMessageSequenceNumberDatabaseAccessor.get()
        }
    }
    //</editor-fold>

    @Bean
    fun azureMarketOrderDatabaseAccessor(@Value("\${azure.market.order.table}") tableName: String)
            : MarketOrderDatabaseAccessor {
        return AzureMarketOrderDatabaseAccessor(config.matchingEngine.db.hTradesConnString, tableName)
    }

    @Bean
    fun azureReservedVolumesDatabaseAccessor(@Value("\${azure.reserved.volumes.table}") tableName: String)
            : ReservedVolumesDatabaseAccessor {
        return AzureReservedVolumesDatabaseAccessor(config.matchingEngine.db.reservedVolumesConnString, tableName)
    }

    @Bean
    fun azureSettingsDatabaseAccessor(@Value("\${azure.settings.database.accessor.table}") tableName: String)
            : SettingsDatabaseAccessor {
        return AzureSettingsDatabaseAccessor(config.matchingEngine.db.matchingEngineConnString, tableName)
    }

    @Bean
    fun settingsHistoryDatabaseAccessor(@Value("\${azure.settings.history.database.accessor.table}") tableName: String): SettingsHistoryDatabaseAccessor {
        return AzureSettingsHistoryDatabaseAccessor(config.matchingEngine.db.matchingEngineConnString, tableName)
    }

    @Bean
    @Profile("default")
    fun azureMonitoringDatabaseAccessor(
        @Value("\${azure.monitoring.table}") monitoringTable: String,
        @Value("\${azure.performance.table}") performanceTable: String
    )
            : MonitoringDatabaseAccessor {
        return AzureMonitoringDatabaseAccessor(
            config.matchingEngine.db.monitoringConnString,
            monitoringTable,
            performanceTable
        )
    }

    @Bean
    fun grpcDictionariesDatabaseAccessor(): DictionariesDatabaseAccessor {
        return GrpcDictionariesDatabaseAccessor(config.matchingEngine.grpcEndpoints.dictionariesConnection)
    }
    //</editor-fold>

    //<editor-fold desc="File db accessors">
    @Bean
    fun fileOrderBookDatabaseAccessor()
            : OrderBookDatabaseAccessor {
        return FileOrderBookDatabaseAccessor(config.matchingEngine.orderBookPath)
    }

    @Bean
    fun fileProcessedMessagesDatabaseAccessor()
            : FileProcessedMessagesDatabaseAccessor {
        return FileProcessedMessagesDatabaseAccessor(
            config.matchingEngine.processedMessagesPath,
            config.matchingEngine.processedMessagesInterval
        )
    }

    @Bean
    fun fileStopOrderBookDatabaseAccessor(): FileStopOrderBookDatabaseAccessor {
        return FileStopOrderBookDatabaseAccessor(config.matchingEngine.stopOrderBookPath)
    }
    //</editor-fold>

    //<editor-fold desc="Persist publishers>
    @Bean
    fun persistedWalletsApplicationEventPublisher(
        updatedWalletsQueue: BlockingQueue<AccountPersistEvent>,
        listeners: Optional<List<QueueConsumer<AccountPersistEvent>?>>
    ): SimpleApplicationEventPublisher<AccountPersistEvent> {
        return ApplicationEventPublisherImpl(updatedWalletsQueue, listeners)
    }

    @Bean
    fun persistedStopOrdersApplicationEventPublisher(
        updatedStopOrderBooksQueue: BlockingQueue<StopOrderBookPersistEvent>,
        listeners: Optional<List<QueueConsumer<StopOrderBookPersistEvent>?>>
    ): SimpleApplicationEventPublisher<StopOrderBookPersistEvent> {
        return ApplicationEventPublisherImpl(updatedStopOrderBooksQueue, listeners)
    }

    @Bean
    fun persistedOrdersApplicationEventPublisher(
        updatedOrderBooksQueue: BlockingQueue<OrderBookPersistEvent>,
        listeners: Optional<List<QueueConsumer<OrderBookPersistEvent>?>>
    ): SimpleApplicationEventPublisher<OrderBookPersistEvent> {
        return ApplicationEventPublisherImpl(updatedOrderBooksQueue, listeners)
    }
    //</editor-fold>
}