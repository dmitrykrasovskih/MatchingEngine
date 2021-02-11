package com.lykke.utils.queue.azure

import com.lykke.utils.logging.ThrottlingLogger
import com.lykke.utils.queue.QueueWriter
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.queue.CloudQueue
import com.microsoft.azure.storage.queue.CloudQueueMessage

internal class AzureQueueWriter(queueConnectionString: String, queueName: String) : QueueWriter {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AzureQueueWriter::class.java.name)
    }

    private val outQueue: CloudQueue

    override fun write(data: String) {
        try {
            outQueue.addMessage(CloudQueueMessage(data))
        } catch (e: Exception) {
            LOGGER.error("Unable to enqueue message to azure queue: $data", e)
        }
    }

    init {
        val storageAccount = CloudStorageAccount.parse(queueConnectionString)
        val queueClient = storageAccount.createCloudQueueClient()
        outQueue = queueClient.getQueueReference(queueName)
        if (!outQueue.exists()) {
            throw Exception("Azure $queueName queue does not exists")
        }
    }
}