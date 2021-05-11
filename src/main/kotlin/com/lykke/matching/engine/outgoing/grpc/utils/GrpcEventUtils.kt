package com.swisschain.matching.engine.outgoing.grpc.utils

class GrpcEventUtils {
    companion object {
        fun getClientEventConsumerQueueName(exchangeName: String, index: Int): String {
            return "client_queue_${exchangeName}_$index"
        }

        fun getTrustedClientsEventConsumerQueueName(exchangeName: String, index: Int): String {
            return "trusted_client_queue_${exchangeName}_$index"
        }

        fun getDatabaseLogQueueName(exchangeName: String, index: Int): String {
            return "database_grpc_log_${exchangeName}_$index"
        }
    }
}