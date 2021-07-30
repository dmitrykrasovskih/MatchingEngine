package com.lykke.matching.engine.database

import redis.clients.jedis.Transaction

interface MessageSequenceNumberDatabaseAccessor {
    fun getSequenceNumber(): Long
    fun save(sequenceNumber: Long)
    fun save(transaction: Transaction, sequenceNumber: Long)
}