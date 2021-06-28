package com.lykke.matching.engine.database

import com.lykke.matching.engine.database.redis.accessor.impl.RedisWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.*

@Component
class BalancesDatabaseAccessorsHolderFactory : FactoryBean<BalancesDatabaseAccessorsHolder> {

    @Autowired
    private lateinit var redisWalletDatabaseAccessor: Optional<RedisWalletDatabaseAccessor>

    override fun getObjectType(): Class<*> {
        return BalancesDatabaseAccessorsHolder::class.java
    }

    override fun getObject(): BalancesDatabaseAccessorsHolder {
        val primaryAccessor: WalletDatabaseAccessor = redisWalletDatabaseAccessor.get()
        return BalancesDatabaseAccessorsHolder(primaryAccessor)
    }
}