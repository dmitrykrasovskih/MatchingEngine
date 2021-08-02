package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.CashOperationIdDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Transaction

class RedisCashOperationIdDatabaseAccessor(
    private val cashOperationRedisConnection: RedisConnection,
    private val dbIndex: Int
) : CashOperationIdDatabaseAccessor {
    companion object {
        val LOGGER = ThrottlingLogger.getLogger(RedisCashOperationIdDatabaseAccessor::class.java.name)
        private const val SEPARATOR = ":"
    }

    private var conf = FSTConfiguration.createJsonConfiguration()

    override fun getProcessedMessage(type: String, id: String): ProcessedMessage? {
        var result: ProcessedMessage? = null

        cashOperationRedisConnection.resource { jedis ->
            jedis.select(dbIndex)
            val value = jedis.get(getKey(type, id))
            if (value != null) {
                result = conf.asObject(value.toByteArray()) as ProcessedMessage
            }
        }

        return result
    }

    fun save(transaction: Transaction, message: ProcessedMessage) {
        transaction.select(dbIndex)
        transaction.set(getKey(message.type.toString(), message.messageId), conf.asJsonString(message))
    }

    private fun getKey(type: String, id: String): String {
        return type + SEPARATOR + id
    }
}
