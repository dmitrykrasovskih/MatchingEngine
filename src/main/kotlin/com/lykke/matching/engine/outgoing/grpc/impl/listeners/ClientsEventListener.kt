package com.lykke.matching.engine.outgoing.grpc.impl.listeners

import com.lykke.matching.engine.database.azure.AzureMessageLogDatabaseAccessor
import com.lykke.matching.engine.logging.DatabaseLogger
import com.lykke.matching.engine.outgoing.grpc.impl.publishers.GrpcEventPublisher
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.utils.config.Config
import com.swisschain.matching.engine.outgoing.grpc.utils.GrpcEventUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class ClientsEventListener {
    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @Value("\${azure.logs.blob.container}")
    private lateinit var logBlobName: String

    @Value("\${azure.logs.clients.events.table}")
    private lateinit var logTable: String

    @PostConstruct
    fun initGrpcPublisher() {
        config.matchingEngine.grpcEndpoints.outgoingEventsConnections.forEachIndexed { index, grpcConnectionString ->
            val clientsEventConsumerQueueName =
                GrpcEventUtils.getClientEventConsumerQueueName(grpcConnectionString, index)
            val queue = applicationContext.getBean(clientsEventConsumerQueueName) as BlockingQueue<Event<*>>
            Thread(
                GrpcEventPublisher(
                    "EventPublisher_$clientsEventConsumerQueueName", queue,
                    clientsEventConsumerQueueName, grpcConnectionString, applicationEventPublisher,
                    DatabaseLogger(
                        AzureMessageLogDatabaseAccessor(
                            config.matchingEngine.db.messageLogConnString,
                            "$logTable$index", "$logBlobName$index"
                        )
                    )
                )
            ).start()
        }
    }
}