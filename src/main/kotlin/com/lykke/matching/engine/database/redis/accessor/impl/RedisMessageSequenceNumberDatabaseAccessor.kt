package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.MessageSequenceNumberDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import redis.clients.jedis.Transaction

class RedisMessageSequenceNumberDatabaseAccessor(
    private val key: String,
    private val redisConnection: RedisConnection,
    private val dbIndex: Int
) : MessageSequenceNumberDatabaseAccessor {
    override fun getSequenceNumber(): Long {
        var result = 0L

        redisConnection.resource { jedis ->
            jedis.select(dbIndex)
            result = jedis[key]?.toLong() ?: 0
        }

        return result
    }

    override fun save(sequenceNumber: Long) {
        redisConnection.transactionalResource { transaction ->
            transaction.select(dbIndex)
            transaction.set(key, sequenceNumber.toString())
            transaction.exec()
        }
    }

    override fun save(transaction: Transaction, sequenceNumber: Long) {
        transaction.select(dbIndex)
        transaction.set(key, sequenceNumber.toString())
    }
}