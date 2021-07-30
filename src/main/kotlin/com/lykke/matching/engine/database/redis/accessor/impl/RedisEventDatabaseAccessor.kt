package com.lykke.matching.engine.database.redis.accessor.impl

import com.lykke.matching.engine.database.EventDatabaseAccessor
import com.lykke.matching.engine.database.redis.connection.RedisConnection
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import org.apache.log4j.Logger
import org.nustaq.serialization.FSTConfiguration
import redis.clients.jedis.Transaction
import java.util.*
import java.util.stream.Collectors

class RedisEventDatabaseAccessor(
    private val redisConnection: RedisConnection,
    private val dbIndex: Int,
    private val timeToLive: Int
) : EventDatabaseAccessor {

    companion object {
        private val LOGGER = Logger.getLogger(RedisEventDatabaseAccessor::class.java.name)
    }

    private val conf = FSTConfiguration.createDefaultConfiguration()

    override fun save(event: Event<*>) {
        //do nothing, use transaction
    }

    fun save(transaction: Transaction, event: Event<*>) {
        transaction.setex(event.sequenceNumber().toString().toByteArray(), timeToLive, conf.asByteArray(event))
    }

    override fun loadEvents(startSequenceId: Long): List<Event<*>> {
        val result = LinkedList<Event<*>>()
        redisConnection.resource { jedis ->
            jedis.select(dbIndex)

            val keys =
                jedis.keys("*").stream().filter { e -> e.toLong() > startSequenceId }.collect(Collectors.toList())

            val values = if (keys.isNotEmpty())
                jedis.mget(*keys.map { it.toByteArray() }.toTypedArray())
            else emptyList()

            values.forEachIndexed { index, value ->
                val key = keys[index]
                try {
                    if (value == null) {
                        LOGGER.error("Stored message does not exist, sequence id: $key")
                    } else {
                        val event = conf.asObject(value) as Event<*>

                        if (key.toLong() != event.sequenceNumber()) {
                            LOGGER.error("Invalid sequence id, key: $key, event: ${event.sequenceNumber()}")
                        } else {
                            result.add(event)
                        }
                    }
                } catch (e: Exception) {
                    val message = "Unable to load stored message, sequence id: $key"
                    LOGGER.error(message, e)
                }
            }
        }
        return result
    }
}