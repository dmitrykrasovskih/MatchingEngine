package com.lykke.matching.engine.performance

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.daos.LkkTrade
import com.lykke.matching.engine.daos.TradeInfo
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.incoming.parsers.impl.*
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.ExecutionEventSender
import com.lykke.matching.engine.order.ExecutionPersistenceService
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.process.*
import com.lykke.matching.engine.order.process.common.LimitOrdersCancellerImpl
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.transaction.ExecutionEventsSequenceNumbersGenerator
import com.lykke.matching.engine.outgoing.messages.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.LimitOrdersReport
import com.lykke.matching.engine.outgoing.messages.MarketOrderWithTrades
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.business.impl.StopOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.input.impl.LimitOrderInputValidatorImpl
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.utils.logging.ThrottlingLogger
import org.mockito.Mockito
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

abstract class AbstractPerformanceTest {

    companion object {
        const val REPEAT_TIMES = 100
        private val LOGGER = ThrottlingLogger.getLogger(AbstractPerformanceTest::class.java.name)
    }

    protected val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
    protected lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    protected lateinit var singleLimitOrderService: SingleLimitOrderService
    private lateinit var genericStopLimitOrderService: GenericStopLimitOrderService
    protected lateinit var genericLimitOrderService: GenericLimitOrderService
    protected lateinit var multiLimitOrderService: MultiLimitOrderService
    protected lateinit var marketOrderService: MarketOrderService

    private lateinit var assetsHolder: AssetsHolder
    protected lateinit var balancesHolder: BalancesHolder
    private lateinit var assetsPairsHolder: AssetsPairsHolder
    private lateinit var assetCache: AssetsCache
    private lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    private val secondaryOrdersDatabaseAccessor = TestFileOrderDatabaseAccessor()
    protected val primaryOrdersDatabaseAccessor = TestOrderBookDatabaseAccessor(secondaryOrdersDatabaseAccessor)
    private var ordersDatabaseAccessorsHolder =
        OrdersDatabaseAccessorsHolder(primaryOrdersDatabaseAccessor, secondaryOrdersDatabaseAccessor)

    private val secondaryStopOrdersDatabaseAccessor = TestFileStopOrderDatabaseAccessor()
    private val primaryStopOrdersDatabaseAccessor =
        TestStopOrderBookDatabaseAccessor(secondaryStopOrdersDatabaseAccessor)

    private var stopOrdersDatabaseAccessorsHolder =
        StopOrdersDatabaseAccessorsHolder(primaryStopOrdersDatabaseAccessor, secondaryStopOrdersDatabaseAccessor)
    private var messageProcessingStatusHolder = Mockito.mock(MessageProcessingStatusHolder::class.java)

    private lateinit var assetPairsCache: AssetPairsCache
    private lateinit var applicationSettingsHolder: ApplicationSettingsHolder
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache
    private lateinit var persistenceManager: PersistenceManager

    private var rabbitEventsQueue = LinkedBlockingQueue<Event<*>>()
    private var rabbitTrustedClientsEventsQueue = LinkedBlockingQueue<ExecutionEvent>()

    private lateinit var singleLimitOrderContextParser: SingleLimitOrderContextParser
    private lateinit var cashInOutContextParser: CashInOutContextParser
    private lateinit var cashTransferContextParser: CashTransferContextParser

    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    private lateinit var feeProcessor: FeeProcessor
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    protected lateinit var messageBuilder: MessageBuilder

    val balanceUpdateQueue = LinkedBlockingQueue<BalanceUpdate>()

    private val clientLimitOrdersQueue = LinkedBlockingQueue<LimitOrdersReport>()

    private val lkkTradesQueue = LinkedBlockingQueue<List<LkkTrade>>()

    private val orderBookQueue = LinkedBlockingQueue<OrderBook>()

    private val rabbitOrderBookQueue = LinkedBlockingQueue<OrderBook>()

    private val rabbitSwapQueue = LinkedBlockingQueue<MarketOrderWithTrades>()

    private val trustedClientsLimitOrdersQueue = LinkedBlockingQueue<LimitOrdersReport>()

    private val tradeInfoQueue = LinkedBlockingQueue<TradeInfo>()

    private fun clearMessageQueues() {
        rabbitEventsQueue.clear()
        rabbitTrustedClientsEventsQueue.clear()
        balanceUpdateQueue.clear()
        clientLimitOrdersQueue.clear()
        lkkTradesQueue.clear()
        orderBookQueue.clear()
        rabbitOrderBookQueue.clear()
        rabbitSwapQueue.clear()
        trustedClientsLimitOrdersQueue.clear()
        tradeInfoQueue.clear()
    }

    open fun initServices() {
        clearMessageQueues()
        testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
        applicationSettingsCache = ApplicationSettingsCache(testSettingsDatabaseAccessor) {}
        applicationSettingsHolder = ApplicationSettingsHolder(applicationSettingsCache)
        val limitOrdersCanceller = LimitOrdersCancellerImpl(applicationSettingsHolder)

        assetCache = AssetsCache(testDictionariesDatabaseAccessor)
        assetsHolder = AssetsHolder(assetCache)
        balancesDatabaseAccessorsHolder = BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor(), null)
        persistenceManager = TestPersistenceManager(
            balancesDatabaseAccessorsHolder.primaryAccessor,
            ordersDatabaseAccessorsHolder,
            stopOrdersDatabaseAccessorsHolder
        )
        balancesHolder = BalancesHolder(
            balancesDatabaseAccessorsHolder,
            persistenceManager,
            assetsHolder,
            balanceUpdateQueue,
            applicationSettingsHolder
        )

        testBalanceHolderWrapper =
            TestBalanceHolderWrapper(BalanceUpdateHandlerTest(balanceUpdateQueue), balancesHolder)
        assetPairsCache = AssetPairsCache(testDictionariesDatabaseAccessor)
        assetsPairsHolder = AssetsPairsHolder(assetPairsCache)

        expiryOrdersQueue = ExpiryOrdersQueue()
        genericLimitOrderService = GenericLimitOrderService(
            ordersDatabaseAccessorsHolder,
            Optional.of(tradeInfoQueue),
            expiryOrdersQueue
        )

        feeProcessor = FeeProcessor(assetsHolder, assetsPairsHolder, genericLimitOrderService)

        val messageSequenceNumberHolder = MessageSequenceNumberHolder(TestMessageSequenceNumberDatabaseAccessor())
        val notificationSender = MessageSender(rabbitEventsQueue, rabbitTrustedClientsEventsQueue)
        val limitOrderInputValidator = LimitOrderInputValidatorImpl(applicationSettingsHolder)
        singleLimitOrderContextParser =
            SingleLimitOrderContextParser(assetsPairsHolder, assetsHolder, applicationSettingsHolder, LOGGER)
        cashInOutContextParser = CashInOutContextParser(assetsHolder)
        cashTransferContextParser = CashTransferContextParser(assetsHolder)

        messageBuilder = MessageBuilder(
            singleLimitOrderContextParser,
            cashInOutContextParser,
            cashTransferContextParser,
            LimitOrderCancelOperationContextParser(),
            LimitOrderMassCancelOperationContextParser()
        )

        genericStopLimitOrderService =
            GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder, expiryOrdersQueue)

        val executionEventsSequenceNumbersGenerator =
            ExecutionEventsSequenceNumbersGenerator(messageSequenceNumberHolder)
        val executionPersistenceService = ExecutionPersistenceService(persistenceManager)
        val executionEventSender = ExecutionEventSender(
            notificationSender,
            clientLimitOrdersQueue,
            trustedClientsLimitOrdersQueue,
            rabbitSwapQueue,
            lkkTradesQueue,
            genericLimitOrderService,
            orderBookQueue,
            rabbitOrderBookQueue
        )
        val executionDataApplyService = ExecutionDataApplyService(
            executionEventsSequenceNumbersGenerator,
            executionPersistenceService,
            executionEventSender
        )

        val executionContextFactory = ExecutionContextFactory(
            balancesHolder,
            genericLimitOrderService,
            genericStopLimitOrderService,
            assetsHolder
        )

        val matchingResultHandlingHelper = MatchingResultHandlingHelper(applicationSettingsHolder)

        val matchingEngine = MatchingEngine(genericLimitOrderService, feeProcessor)

        val limitOrderProcessor = LimitOrderProcessor(
            limitOrderInputValidator,
            LimitOrderBusinessValidatorImpl(),
            applicationSettingsHolder,
            matchingEngine,
            matchingResultHandlingHelper
        )

        val stopOrderProcessor = StopLimitOrderProcessor(
            limitOrderInputValidator,
            StopOrderBusinessValidatorImpl(),
            applicationSettingsHolder,
            limitOrderProcessor
        )

        val genericLimitOrdersProcessor = GenericLimitOrdersProcessor(limitOrderProcessor, stopOrderProcessor)

        val stopOrderBookProcessor = StopOrderBookProcessor(limitOrderProcessor, applicationSettingsHolder)

        val previousLimitOrdersProcessor =
            PreviousLimitOrdersProcessor(genericLimitOrderService, genericStopLimitOrderService, limitOrdersCanceller)

        singleLimitOrderService = SingleLimitOrderService(
            executionContextFactory,
            genericLimitOrdersProcessor,
            stopOrderBookProcessor,
            executionDataApplyService,
            previousLimitOrdersProcessor,
            balancesHolder
        )

        multiLimitOrderService = MultiLimitOrderService(
            executionContextFactory,
            genericLimitOrdersProcessor,
            stopOrderBookProcessor,
            executionDataApplyService,
            previousLimitOrdersProcessor,
            assetsHolder,
            assetsPairsHolder,
            balancesHolder,
            applicationSettingsHolder,
            messageProcessingStatusHolder
        )

        val marketOrderValidator = MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, applicationSettingsHolder)
        marketOrderService = MarketOrderService(
            matchingEngine,
            executionContextFactory,
            stopOrderBookProcessor,
            executionDataApplyService,
            matchingResultHandlingHelper,
            genericLimitOrderService,
            assetsPairsHolder,
            rabbitSwapQueue,
            marketOrderValidator,
            applicationSettingsHolder,
            messageSequenceNumberHolder,
            notificationSender,
            messageProcessingStatusHolder,
            balancesHolder
        )

    }
}