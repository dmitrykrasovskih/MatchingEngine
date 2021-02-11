package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.Storage
import com.lykke.matching.engine.database.file.FileOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.accessor.impl.RedisOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.Optional

@Component
class OrdersDatabaseAccessorsHolderFactory : FactoryBean<OrdersDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var initialLoadingRedisConnection: Optional<RedisConnection>

    override fun getObjectType(): Class<*> {
        return OrdersDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): OrdersDatabaseAccessorsHolder {
        return when (config.matchingEngine.storage) {
            Storage.Azure ->
                OrdersDatabaseAccessorsHolder(FileOrderBookDatabaseAccessor(config.matchingEngine.orderBookPath), null)
            Storage.Redis ->
                OrdersDatabaseAccessorsHolder(RedisOrderBookDatabaseAccessor(initialLoadingRedisConnection.get(), config.matchingEngine.redis.ordersDatabase),
                        if (config.matchingEngine.writeOrdersToSecondaryDb)
                            FileOrderBookDatabaseAccessor(config.matchingEngine.secondaryOrderBookPath)
                        else null)
            Storage.RedisWithoutOrders ->
                OrdersDatabaseAccessorsHolder(FileOrderBookDatabaseAccessor(config.matchingEngine.orderBookPath), null)
        }
    }
}