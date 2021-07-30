package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.messages.wrappers.*
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.LinkedBlockingQueue

@Configuration
class QueueConfig {

    //<editor-fold desc="Outgoing queues">
    @Bean
    @OutgoingQueue
    fun clientsEventsQueue(): BlockingDeque<Event<*>> {
        return LinkedBlockingDeque()
    }

    @Bean
    @OutgoingQueue
    fun trustedClientsEventsQueue(): BlockingDeque<Event<*>> {
        return LinkedBlockingDeque()
    }
    //</editor-fold>


    //<editor-fold desc="Input queues">
    @Bean
    @InputQueue
    fun limitOrderInputQueue(): BlockingQueue<SingleLimitOrderMessageWrapper> {
        return LinkedBlockingQueue<SingleLimitOrderMessageWrapper>()
    }

    @Bean
    @InputQueue
    fun cashInOutInputQueue(): BlockingQueue<CashInOutOperationMessageWrapper> {
        return LinkedBlockingQueue<CashInOutOperationMessageWrapper>()
    }

    @Bean
    @InputQueue
    fun cashTransferInputQueue(): BlockingQueue<CashTransferOperationMessageWrapper> {
        return LinkedBlockingQueue<CashTransferOperationMessageWrapper>()
    }

    @Bean
    @InputQueue
    fun cashSwapInputQueue(): BlockingQueue<CashSwapOperationMessageWrapper> {
        return LinkedBlockingQueue()
    }

    @Bean
    @InputQueue
    fun reservedCashInOutInputQueue(): BlockingQueue<ReservedCashInOutOperationMessageWrapper> {
        return LinkedBlockingQueue<ReservedCashInOutOperationMessageWrapper>()
    }

    @Bean
    @InputQueue
    fun limitOrderCancelInputQueue(): BlockingQueue<LimitOrderCancelMessageWrapper> {
        return LinkedBlockingQueue<LimitOrderCancelMessageWrapper>()
    }

    @Bean
    @InputQueue
    fun limitOrderMassCancelInputQueue(): BlockingQueue<LimitOrderMassCancelMessageWrapper> {
        return LinkedBlockingQueue<LimitOrderMassCancelMessageWrapper>()
    }

    @Bean
    @InputQueue
    fun preProcessedMessageQueue(): BlockingQueue<MessageWrapper> {
        return LinkedBlockingQueue<MessageWrapper>()
    }
    //</editor-fold>


    //<editor-fold desc="Etc queues">
    @Bean
    fun orderBookQueue(): BlockingQueue<OrderBook> {
        return LinkedBlockingQueue<OrderBook>()
    }

    @Bean
    fun dbTransferOperationQueue(): BlockingQueue<TransferOperation> {
        return LinkedBlockingQueue<TransferOperation>()
    }
    //</editor-fold>
}