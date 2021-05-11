package com.lykke.matching.engine.config.spring.grpc

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.publishers.dispatchers.OutgoingEventDispatcher
import com.lykke.matching.engine.utils.config.Config
import com.swisschain.matching.engine.outgoing.grpc.utils.GrpcEventUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue

@Configuration
class GrpcConfiguration {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Bean
    fun trustedClientsEventsDispatcher(trustedClientsEventsQueue: BlockingDeque<ExecutionEvent>): OutgoingEventDispatcher<ExecutionEvent> {
        return OutgoingEventDispatcher(
            "TrustedClientEventsDispatcher",
            trustedClientsEventsQueue,
            trustedQueueNameToQueue()
        )
    }

    @Bean
    fun clientEventsDispatcher(clientsEventsQueue: BlockingDeque<Event<*>>): OutgoingEventDispatcher<Event<*>> {
        return OutgoingEventDispatcher("ClientEventsDispatcher", clientsEventsQueue, clientQueueNameToQueue())
    }

    @Bean
    fun trustedQueueNameToQueue(): Map<String, BlockingQueue<ExecutionEvent>> {
        val consumerNameToQueue = HashMap<String, BlockingQueue<ExecutionEvent>>()
        config.matchingEngine.grpcEndpoints.outgoingTrustedClientsEventsConnections.forEachIndexed { index, grpcConnectionString ->
            val trustedClientsEventConsumerQueueName =
                GrpcEventUtils.getTrustedClientsEventConsumerQueueName(grpcConnectionString, index)
            val queue =
                applicationContext.getBean(trustedClientsEventConsumerQueueName) as BlockingQueue<ExecutionEvent>

            consumerNameToQueue[trustedClientsEventConsumerQueueName] = queue
        }

        return consumerNameToQueue
    }

    @Bean
    fun clientQueueNameToQueue(): Map<String, BlockingQueue<Event<*>>> {
        val consumerNameToQueue = HashMap<String, BlockingQueue<Event<*>>>()
        config.matchingEngine.grpcEndpoints.outgoingEventsConnections.forEachIndexed { index, grpcConnectionString ->
            val clientsEventConsumerQueueName =
                GrpcEventUtils.getClientEventConsumerQueueName(grpcConnectionString, index)

            val queue = applicationContext.getBean(clientsEventConsumerQueueName) as BlockingQueue<Event<*>>
            consumerNameToQueue[clientsEventConsumerQueueName] = queue
        }

        return consumerNameToQueue
    }
}

