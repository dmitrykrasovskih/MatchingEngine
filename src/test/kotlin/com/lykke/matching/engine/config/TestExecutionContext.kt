package com.lykke.matching.engine.config

import com.lykke.matching.engine.database.PersistenceManager
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.ExecutionEventSender
import com.lykke.matching.engine.order.ExecutionPersistenceService
import com.lykke.matching.engine.order.process.*
import com.lykke.matching.engine.order.process.common.*
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.transaction.ExecutionEventsSequenceNumbersGenerator
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.services.GenericStopLimitOrderService
import com.lykke.matching.engine.services.MessageSender
import com.lykke.matching.engine.services.validators.business.LimitOrderBusinessValidator
import com.lykke.matching.engine.services.validators.business.StopOrderBusinessValidator
import com.lykke.matching.engine.services.validators.input.LimitOrderInputValidator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue

@Configuration
class TestExecutionContext {

    @Bean
    fun matchingEngine(
        genericLimitOrderService: GenericLimitOrderService,
        balancesHolder: BalancesHolder,
        feeProcessor: FeeProcessor
    ): MatchingEngine {
        return MatchingEngine(
            genericLimitOrderService,
            feeProcessor
        )
    }

    @Bean
    fun executionContextFactory(
        balancesHolder: BalancesHolder,
        genericLimitOrderService: GenericLimitOrderService,
        genericStopLimitOrderService: GenericStopLimitOrderService,
        assetsHolder: AssetsHolder
    ): ExecutionContextFactory {
        return ExecutionContextFactory(
            balancesHolder,
            genericLimitOrderService,
            genericStopLimitOrderService,
            assetsHolder
        )
    }

    @Bean
    fun executionEventsSequenceNumbersGenerator(messageSequenceNumberHolder: MessageSequenceNumberHolder): ExecutionEventsSequenceNumbersGenerator {
        return ExecutionEventsSequenceNumbersGenerator(messageSequenceNumberHolder)
    }

    @Bean
    fun executionPersistenceService(persistenceManager: PersistenceManager): ExecutionPersistenceService {
        return ExecutionPersistenceService(persistenceManager)
    }

    @Bean
    fun executionEventSender(
        messageSender: MessageSender,
        clientLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
        trustedClientsLimitOrdersQueue: BlockingQueue<LimitOrdersReport>,
        rabbitSwapQueue: BlockingQueue<MarketOrderWithTrades>,
        genericLimitOrderService: GenericLimitOrderService,
        orderBookQueue: BlockingQueue<OrderBook>,
        rabbitOrderBookQueue: BlockingQueue<OrderBook>
    ): ExecutionEventSender {
        return ExecutionEventSender(
            messageSender,
            genericLimitOrderService
        )
    }

    @Bean
    fun executionDataApplyService(
        executionEventsSequenceNumbersGenerator: ExecutionEventsSequenceNumbersGenerator,
        executionPersistenceService: ExecutionPersistenceService,
        executionEventSender: ExecutionEventSender
    ): ExecutionDataApplyService {
        return ExecutionDataApplyService(
            executionEventsSequenceNumbersGenerator,
            executionPersistenceService,
            executionEventSender
        )
    }

    @Bean
    fun limitOrderProcessor(
        limitOrderInputValidator: LimitOrderInputValidator,
        limitOrderBusinessValidator: LimitOrderBusinessValidator,
        applicationSettingsHolder: ApplicationSettingsHolder,
        matchingEngine: MatchingEngine,
        matchingResultHandlingHelper: MatchingResultHandlingHelper
    ): LimitOrderProcessor {
        return LimitOrderProcessor(
            limitOrderInputValidator,
            limitOrderBusinessValidator,
            applicationSettingsHolder,
            matchingEngine,
            matchingResultHandlingHelper
        )
    }

    @Bean
    fun stopLimitOrdersProcessor(
        limitOrderInputValidator: LimitOrderInputValidator,
        stopOrderBusinessValidator: StopOrderBusinessValidator,
        applicationSettingsHolder: ApplicationSettingsHolder,
        limitOrderProcessor: LimitOrderProcessor
    ): StopLimitOrderProcessor {
        return StopLimitOrderProcessor(
            limitOrderInputValidator,
            stopOrderBusinessValidator,
            applicationSettingsHolder,
            limitOrderProcessor
        )
    }

    @Bean
    fun genericLimitOrdersProcessor(
        limitOrderProcessor: LimitOrderProcessor,
        stopLimitOrdersProcessor: StopLimitOrderProcessor
    ): GenericLimitOrdersProcessor {
        return GenericLimitOrdersProcessor(limitOrderProcessor, stopLimitOrdersProcessor)
    }

    @Bean
    fun stopOrderBookProcessor(
        limitOrderProcessor: LimitOrderProcessor,
        applicationSettingsHolder: ApplicationSettingsHolder
    ): StopOrderBookProcessor {
        return StopOrderBookProcessor(limitOrderProcessor, applicationSettingsHolder)
    }

    @Bean
    fun matchingResultHandlingHelper(applicationSettingsHolder: ApplicationSettingsHolder): MatchingResultHandlingHelper {
        return MatchingResultHandlingHelper(applicationSettingsHolder)
    }

    @Bean
    fun previousLimitOrdersProcessor(
        genericLimitOrderService: GenericLimitOrderService,
        genericStopLimitOrderService: GenericStopLimitOrderService,
        limitOrdersCanceller: LimitOrdersCanceller
    ): PreviousLimitOrdersProcessor {
        return PreviousLimitOrdersProcessor(
            genericLimitOrderService,
            genericStopLimitOrderService,
            limitOrdersCanceller
        )
    }

    @Bean
    fun limitOrdersCanceller(applicationSettingsHolder: ApplicationSettingsHolder): LimitOrdersCanceller {
        return LimitOrdersCancellerImpl(applicationSettingsHolder)
    }

    @Bean
    fun limitOrdersCancelExecutor(
        assetsPairsHolder: AssetsPairsHolder,
        executionContextFactory: ExecutionContextFactory,
        limitOrdersCanceller: LimitOrdersCanceller,
        stopOrderBookProcessor: StopOrderBookProcessor,
        executionDataApplyService: ExecutionDataApplyService
    ): LimitOrdersCancelExecutor {
        return LimitOrdersCancelExecutorImpl(
            assetsPairsHolder,
            executionContextFactory,
            limitOrdersCanceller,
            stopOrderBookProcessor,
            executionDataApplyService
        )
    }

}