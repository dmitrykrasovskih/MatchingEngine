package com.lykke.matching.engine.holders

import com.lykke.matching.engine.database.redis.accessor.impl.RedisStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class StopOrdersDatabaseAccessorsHolderFactory : FactoryBean<StopOrdersDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var initialLoadingRedisConnection: Optional<RedisConnection>

    override fun getObjectType(): Class<*> {
        return StopOrdersDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): StopOrdersDatabaseAccessorsHolder {
        return StopOrdersDatabaseAccessorsHolder(
            RedisStopOrderBookDatabaseAccessor(
                initialLoadingRedisConnection.get(),
                config.matchingEngine.redis.ordersDatabase
            )
        )
    }
}