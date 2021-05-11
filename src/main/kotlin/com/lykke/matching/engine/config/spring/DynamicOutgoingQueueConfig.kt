package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.config.ConfigFactory
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages
import com.swisschain.matching.engine.outgoing.grpc.utils.GrpcEventUtils
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.beans.factory.support.AutowireCandidateQualifier
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.beans.factory.support.RootBeanDefinition
import org.springframework.context.EnvironmentAware
import org.springframework.context.annotation.Configuration
import org.springframework.core.ResolvableType
import org.springframework.core.env.Environment
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

@Configuration("dynamicOutgoingQueueConfig")
class DynamicOutgoingQueueConfig : BeanFactoryPostProcessor, EnvironmentAware {
    private lateinit var environment: Environment

    private var clientEvents: BlockingQueue<Event<*>>? = null
    private var trustedEvents: BlockingQueue<ExecutionEvent>? = null
    private var databaseLogQueue: BlockingQueue<OutgoingMessages.MessageWrapper>? = null

    override fun setEnvironment(environment: Environment) {
        this.environment = environment
    }

    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        registerClientEventsQueue(beanFactory as DefaultListableBeanFactory)
        registerTrustedClientsEventsQueue(beanFactory)
        registerClientDatabaseLogQueues(beanFactory)
    }

    @OutgoingQueue
    private fun registerClientEventsQueue(factory: DefaultListableBeanFactory) {
        val config = ConfigFactory.getConfig(environment)
        registerQueue(
            factory,
            config.matchingEngine.grpcEndpoints.outgoingEventsConnections,
            "clientEvents",
            GrpcEventUtils.Companion::getClientEventConsumerQueueName
        )
    }

    @OutgoingQueue
    private fun registerTrustedClientsEventsQueue(factory: DefaultListableBeanFactory) {
        val config = ConfigFactory.getConfig(environment)
        registerQueue(
            factory,
            config.matchingEngine.grpcEndpoints.outgoingTrustedClientsEventsConnections,
            "trustedEvents",
            GrpcEventUtils.Companion::getTrustedClientsEventConsumerQueueName
        )
    }

    @DataQueue
    private fun registerClientDatabaseLogQueues(factory: DefaultListableBeanFactory) {
        val config = ConfigFactory.getConfig(environment)
        registerQueue(
            factory,
            config.matchingEngine.grpcEndpoints.outgoingEventsConnections,
            "databaseLogQueue",
            GrpcEventUtils.Companion::getDatabaseLogQueueName
        )
    }

    private fun registerQueue(
        factory: DefaultListableBeanFactory,
        grpcConnections: Set<String>,
        typeFieldName: String,
        queueNameStrategy: (exchangeName: String, index: Int) -> String
    ) {
        val annotations = DynamicOutgoingQueueConfig::class.java.getDeclaredMethod(
            Throwable().stackTrace[1].methodName,
            DefaultListableBeanFactory::class.java
        )
            .annotations
            .map { it.annotationClass.java }

        val queueBeanDefinition = RootBeanDefinition(LinkedBlockingQueue::class.java)
        queueBeanDefinition.setTargetType(
            ResolvableType.forField(
                DynamicOutgoingQueueConfig::class.java.getDeclaredField(
                    typeFieldName
                )
            )
        )

        annotations.forEach {
            queueBeanDefinition.addQualifier(AutowireCandidateQualifier(it))
        }

        grpcConnections.forEachIndexed { index, grpcConnection ->
            factory.registerBeanDefinition(queueNameStrategy.invoke(grpcConnection, index), queueBeanDefinition)
        }
    }
}