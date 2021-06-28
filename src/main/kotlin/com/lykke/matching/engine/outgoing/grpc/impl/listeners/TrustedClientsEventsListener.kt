package com.lykke.matching.engine.outgoing.grpc.impl.listeners

import com.lykke.matching.engine.outgoing.grpc.impl.publishers.GrpcEventPublisher
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.utils.config.Config
import com.swisschain.matching.engine.outgoing.grpc.utils.GrpcEventUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

@Component
class TrustedClientsEventsListener {
    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Autowired
    private lateinit var applicationEventPublisher: ApplicationEventPublisher

    @PostConstruct
    fun initGrpcPublisher() {
        config.matchingEngine.grpcEndpoints.outgoingTrustedClientsEventsConnections.forEachIndexed { index, grpcConnectionString ->
            val trustedClientsEventConsumerQueue =
                GrpcEventUtils.getTrustedClientsEventConsumerQueueName(grpcConnectionString, index)
            @Suppress("UNCHECKED_CAST") val queue =
                applicationContext.getBean(trustedClientsEventConsumerQueue) as BlockingQueue<Event<*>>
            Thread(
                GrpcEventPublisher(
                    "TrustedClientsEventPublisher_$trustedClientsEventConsumerQueue", queue,
                    trustedClientsEventConsumerQueue, grpcConnectionString, applicationEventPublisher,
                    null
                )
            ).start()
        }
    }
}