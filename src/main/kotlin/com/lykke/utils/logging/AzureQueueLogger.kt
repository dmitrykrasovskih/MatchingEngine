package com.lykke.utils.logging

import com.lykke.utils.queue.QueueWriter
import com.lykke.utils.queue.azure.AzureQueueWriter
import java.util.concurrent.BlockingQueue

internal class AzureQueueLogger(queueConnectionString: String, queueName: String, private val queue: BlockingQueue<LoggableObject>) : Thread() {

    private val queueWriter: QueueWriter

    override fun run() {
        while (true) {
            val obj = queue.take()
            queueWriter.write(obj.getJson())
        }
    }

    init {
        this.queueWriter = AzureQueueWriter(queueConnectionString, queueName)
    }
}