package com.lykke.matching.engine.database

import redis.clients.jedis.Transaction

class TestMessageSequenceNumberDatabaseAccessor : MessageSequenceNumberDatabaseAccessor {
    override fun getSequenceNumber() = 0L
    override fun save(sequenceNumber: Long) {
    }

    override fun save(transaction: Transaction, sequenceNumber: Long) {
    }
}