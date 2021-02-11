package com.lykke.matching.engine.notification

import java.util.concurrent.BlockingQueue

abstract class AbstractQueueWrapper<V> {

    abstract fun getProcessingQueue(): BlockingQueue<*>

    fun getCount(): Int {
        return getProcessingQueue().size
    }

    fun getQueue(): BlockingQueue<V> {
        @Suppress("UNCHECKED_CAST")
        return getProcessingQueue() as BlockingQueue<V>
    }

    fun clear() {
        getProcessingQueue().clear()
    }
}