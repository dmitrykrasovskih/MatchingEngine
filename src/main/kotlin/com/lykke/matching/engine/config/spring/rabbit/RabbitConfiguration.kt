package com.lykke.matching.engine.config.spring.rabbit

import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.rabbit.impl.dispatchers.RabbitEventDispatcher
import com.lykke.matching.engine.outgoing.rabbit.utils.RabbitEventUtils
import com.lykke.matching.engine.utils.config.Config
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue

@Suppress("UNCHECKED_CAST")
@Configuration
class RabbitConfiguration {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Bean
    fun trustedClientsEventsDispatcher(trustedClientsEventsQueue: BlockingDeque<ExecutionEvent>): RabbitEventDispatcher<ExecutionEvent> {
        return RabbitEventDispatcher("TrustedClientEventsDispatcher", trustedClientsEventsQueue, trustedQueueNameToQueue())
    }

    @Bean
    fun clientEventsDispatcher(clientsEventsQueue: BlockingDeque<Event<*>>): RabbitEventDispatcher<Event<*>> {
        return RabbitEventDispatcher("ClientEventsDispatcher", clientsEventsQueue, clientQueueNameToQueue())
    }

    @Bean
    fun trustedQueueNameToQueue(): Map<String, BlockingQueue<ExecutionEvent>> {
        val consumerNameToQueue = HashMap<String, BlockingQueue<ExecutionEvent>>()
        config.matchingEngine.rabbitMqConfigs.trustedClientsEvents.forEachIndexed { index, rabbitConfig ->
            val trustedClientsEventConsumerQueueName = RabbitEventUtils.getTrustedClientsEventConsumerQueueName(rabbitConfig.exchange, index)
            val queue = applicationContext.getBean(trustedClientsEventConsumerQueueName) as BlockingQueue<ExecutionEvent>

            consumerNameToQueue.put(trustedClientsEventConsumerQueueName, queue)
        }

        return consumerNameToQueue
    }

    @Bean
    fun clientQueueNameToQueue(): Map<String, BlockingQueue<Event<*>>> {
        val consumerNameToQueue = HashMap<String, BlockingQueue<Event<*>>>()
        config.matchingEngine.rabbitMqConfigs.events.forEachIndexed { index, rabbitConfig ->
            val clientsEventConsumerQueueName = RabbitEventUtils.getClientEventConsumerQueueName(rabbitConfig.exchange, index)

            val queue = applicationContext.getBean(clientsEventConsumerQueueName) as BlockingQueue<Event<*>>
            consumerNameToQueue.put(clientsEventConsumerQueueName, queue)
        }

        return consumerNameToQueue
    }
}

