package com.lykke.matching.engine.config

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.spring.JsonConfig
import com.lykke.matching.engine.config.spring.QueueConfig
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.*
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.deduplication.ProcessedMessagesCache
import com.lykke.matching.engine.fee.FeeProcessor
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.impl.*
import com.lykke.matching.engine.incoming.preprocessor.impl.CashInOutPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.matching.MatchingEngine
import com.lykke.matching.engine.messages.wrappers.LimitOrderCancelMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.matching.engine.notification.SettingsListener
import com.lykke.matching.engine.notification.TestOrderBookListener
import com.lykke.matching.engine.order.ExecutionDataApplyService
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.process.GenericLimitOrdersProcessor
import com.lykke.matching.engine.order.process.PreviousLimitOrdersProcessor
import com.lykke.matching.engine.order.process.StopOrderBookProcessor
import com.lykke.matching.engine.order.process.common.LimitOrdersCancelExecutor
import com.lykke.matching.engine.order.process.common.MatchingResultHandlingHelper
import com.lykke.matching.engine.order.transaction.ExecutionContextFactory
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.business.*
import com.lykke.matching.engine.services.validators.business.impl.*
import com.lykke.matching.engine.services.validators.impl.MarketOrderValidatorImpl
import com.lykke.matching.engine.services.validators.input.*
import com.lykke.matching.engine.services.validators.input.impl.*
import com.lykke.matching.engine.services.validators.settings.SettingValidator
import com.lykke.matching.engine.services.validators.settings.impl.DisabledFunctionalitySettingValidator
import com.lykke.matching.engine.services.validators.settings.impl.MessageProcessingSwitchSettingValidator
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.balance.ReservedVolumesRecalculator
import com.lykke.matching.engine.utils.monitoring.HealthMonitor
import com.lykke.matching.engine.utils.order.AllOrdersCanceller
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import com.lykke.utils.logging.ThrottlingLogger
import org.mockito.Mockito
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue

@Configuration
@Import(QueueConfig::class, TestExecutionContext::class, JsonConfig::class)
class TestApplicationContext {

    @Bean
    fun threadPoolTaskExecutor(): Executor {
        val threadPoolTaskExecutor = ThreadPoolTaskExecutor()
        threadPoolTaskExecutor.threadNamePrefix = "executor-task"
        threadPoolTaskExecutor.corePoolSize = 2
        threadPoolTaskExecutor.maxPoolSize = 2

        return threadPoolTaskExecutor
    }

    @Bean
    fun balanceHolder(
        balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder,
        persistenceManager: PersistenceManager,
        applicationSettingsHolder: ApplicationSettingsHolder,
        dictionariesDatabaseAccessor: DictionariesDatabaseAccessor
    ): BalancesHolder {
        return BalancesHolder(
            balancesDatabaseAccessorsHolder,
            persistenceManager,
            assetHolder(dictionariesDatabaseAccessor),
            applicationSettingsHolder
        )
    }

    @Bean
    fun assetHolder(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor): AssetsHolder {
        return AssetsHolder(assetCache(dictionariesDatabaseAccessor))
    }

    @Bean
    fun applicationSettingsHolder(applicationSettingsCache: ApplicationSettingsCache): ApplicationSettingsHolder {
        return ApplicationSettingsHolder(applicationSettingsCache)
    }

    @Bean
    fun messageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor: MessageSequenceNumberDatabaseAccessor): MessageSequenceNumberHolder {
        return MessageSequenceNumberHolder(messageSequenceNumberDatabaseAccessor)
    }

    @Bean
    fun notificationSender(
        clientsEventsQueue: BlockingQueue<Event<*>>,
        trustedClientsEventsQueue: BlockingQueue<Event<*>>
    ): MessageSender {
        return MessageSender(clientsEventsQueue, trustedClientsEventsQueue)
    }

    @Bean
    fun reservedVolumesRecalculator(
        ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
        stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
        testReservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor,
        assetHolder: AssetsHolder, assetsPairsHolder: AssetsPairsHolder,
        balancesHolder: BalancesHolder, applicationSettingsHolder: ApplicationSettingsHolder,
        messageSequenceNumberHolder: MessageSequenceNumberHolder,
        messageSender: MessageSender
    ): ReservedVolumesRecalculator {

        return ReservedVolumesRecalculator(
            ordersDatabaseAccessorsHolder, stopOrdersDatabaseAccessorsHolder,
            testReservedVolumesDatabaseAccessor, assetHolder,
            assetsPairsHolder, balancesHolder, applicationSettingsHolder,
            false, messageSequenceNumberHolder, messageSender
        )
    }

    @Bean
    fun testMessageSequenceNumberDatabaseAccessor(): TestMessageSequenceNumberDatabaseAccessor {
        return TestMessageSequenceNumberDatabaseAccessor()
    }

    @Bean
    fun testReservedVolumesDatabaseAccessor(): TestReservedVolumesDatabaseAccessor {
        return TestReservedVolumesDatabaseAccessor()
    }

    @Bean
    fun assetCache(dictionariesDatabaseAccessor: DictionariesDatabaseAccessor): AssetsCache {
        return AssetsCache(dictionariesDatabaseAccessor)
    }

    @Bean
    fun testSettingsDatabaseAccessor(): TestSettingsDatabaseAccessor {
        return TestSettingsDatabaseAccessor()
    }

    @Bean
    fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
        return TestDictionariesDatabaseAccessor()
    }

    @Bean
    fun applicationSettingsCache(
        testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor,
        applicationEventPublisher: ApplicationEventPublisher
    ): ApplicationSettingsCache {
        return ApplicationSettingsCache(testSettingsDatabaseAccessor, applicationEventPublisher)
    }

    @Bean
    fun testBalanceHolderWrapper(
        balancesHolder: BalancesHolder
    ): TestBalanceHolderWrapper {
        return TestBalanceHolderWrapper(balancesHolder)
    }

    @Bean
    fun balancesDatabaseAccessorsHolder(): BalancesDatabaseAccessorsHolder {
        return BalancesDatabaseAccessorsHolder(TestWalletDatabaseAccessor())
    }

    @Bean
    fun ordersDatabaseAccessorsHolder(
        testOrderBookDatabaseAccessor: TestOrderBookDatabaseAccessor
    ): OrdersDatabaseAccessorsHolder {
        return OrdersDatabaseAccessorsHolder(testOrderBookDatabaseAccessor)
    }

    @Bean
    fun testOrderBookDatabaseAccessor(testFileOrderDatabaseAccessor: TestFileOrderDatabaseAccessor): TestOrderBookDatabaseAccessor {
        return TestOrderBookDatabaseAccessor(testFileOrderDatabaseAccessor)
    }

    @Bean
    fun stopOrdersDatabaseAccessorsHolder(
        testStopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor
    ): StopOrdersDatabaseAccessorsHolder {
        return StopOrdersDatabaseAccessorsHolder(testStopOrderBookDatabaseAccessor)
    }

    @Bean
    fun persistenceManager(
        ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
        stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder
    ): PersistenceManager {
        return TestPersistenceManager(
            balancesDatabaseAccessorsHolder().primaryAccessor,
            ordersDatabaseAccessorsHolder,
            stopOrdersDatabaseAccessorsHolder
        )
    }

    @Bean
    fun cashInOutOperationBusinessValidator(balancesHolder: BalancesHolder): CashInOutOperationBusinessValidator {
        return CashInOutOperationBusinessValidatorImpl(balancesHolder)
    }

    @Bean
    fun cashTransferOperationBusinessValidator(balancesHolder: BalancesHolder): CashTransferOperationBusinessValidator {
        return CashTransferOperationBusinessValidatorImpl(balancesHolder)
    }

    @Bean
    fun cashInOutOperationInputValidator(applicationSettingsHolder: ApplicationSettingsHolder): CashInOutOperationInputValidator {
        return CashInOutOperationInputValidatorImpl(applicationSettingsHolder)
    }

    @Bean
    fun reservedCashInOutOperationInputValidator(): ReservedCashInOutOperationValidator {
        return ReservedCashInOutOperationValidatorImpl()
    }

    @Bean
    fun reservedCashInOutOperationBusinessValidator(balancesHolder: BalancesHolder): ReservedCashInOutOperationBusinessValidator {
        return ReservedCashInOutOperationBusinessValidatorImpl(balancesHolder)
    }

    @Bean
    fun cashTransferOperationInputValidator(applicationSettingsHolder: ApplicationSettingsHolder): CashTransferOperationInputValidator {
        return CashTransferOperationInputValidatorImpl(applicationSettingsHolder)
    }

    @Bean
    fun disabledFunctionality(
        assetsHolder: AssetsHolder,
        assetsPairsHolder: AssetsPairsHolder
    ): DisabledFunctionalitySettingValidator {
        return DisabledFunctionalitySettingValidator(assetsHolder, assetsPairsHolder)
    }

    @Bean
    fun cashInOutOperationService(
        balancesHolder: BalancesHolder,
        feeProcessor: FeeProcessor,
        cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator,
        messageSequenceNumberHolder: MessageSequenceNumberHolder,
        messageSender: MessageSender
    ): CashInOutOperationService {
        return CashInOutOperationService(
            balancesHolder,
            feeProcessor,
            cashInOutOperationBusinessValidator,
            messageSequenceNumberHolder,
            messageSender
        )
    }

    @Bean
    fun marketOrderValidator(
        assetsPairsHolder: AssetsPairsHolder,
        assetsHolder: AssetsHolder,
        applicationSettingsHolder: ApplicationSettingsHolder
    ): MarketOrderValidator {
        return MarketOrderValidatorImpl(assetsPairsHolder, assetsHolder, applicationSettingsHolder)
    }

    @Bean
    fun assetPairsCache(testDictionariesDatabaseAccessor: DictionariesDatabaseAccessor): AssetPairsCache {
        return AssetPairsCache(testDictionariesDatabaseAccessor)
    }

    @Bean
    fun assetPairHolder(assetPairsCache: AssetPairsCache): AssetsPairsHolder {
        return AssetsPairsHolder(assetPairsCache)
    }

    @Bean
    fun reservedCashInOutOperation(
        balancesHolder: BalancesHolder,
        assetsHolder: AssetsHolder,
        reservedCashInOutOperationBusinessValidator: ReservedCashInOutOperationBusinessValidator,
        messageSequenceNumberHolder: MessageSequenceNumberHolder,
        messageSender: MessageSender
    ): ReservedCashInOutOperationService {
        return ReservedCashInOutOperationService(
            balancesHolder, reservedCashInOutOperationBusinessValidator, messageSequenceNumberHolder, messageSender
        )
    }

    @Bean
    fun applicationSettingsHistoryDatabaseAccessor(): SettingsHistoryDatabaseAccessor {
        return Mockito.mock(SettingsHistoryDatabaseAccessor::class.java)
    }

    @Bean
    fun applicationSettingsService(
        testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor,
        applicationSettingsCache: ApplicationSettingsCache,
        settingsHistoryDatabaseAccessor: SettingsHistoryDatabaseAccessor,
        applicationEventPublisher: ApplicationEventPublisher
    ): ApplicationSettingsService {
        return ApplicationSettingsServiceImpl(
            testSettingsDatabaseAccessor,
            applicationSettingsCache,
            settingsHistoryDatabaseAccessor,
            applicationEventPublisher
        )
    }

    @Bean
    fun disabledFunctionalityRulesHolder(
        applicationSettingsCache: ApplicationSettingsCache,
        assetsPairsHolder: AssetsPairsHolder
    ): DisabledFunctionalityRulesHolder {
        return DisabledFunctionalityRulesHolder(applicationSettingsCache, assetsPairsHolder)
    }

    @Bean
    fun genericLimitOrderService(
        ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder,
        expiryOrdersQueue: ExpiryOrdersQueue
    ): GenericLimitOrderService {
        return GenericLimitOrderService(
            ordersDatabaseAccessorsHolder,
            expiryOrdersQueue
        )
    }

    @Bean
    fun singleLimitOrderService(
        executionContextFactory: ExecutionContextFactory,
        genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
        stopOrderBookProcessor: StopOrderBookProcessor,
        executionDataApplyService: ExecutionDataApplyService,
        previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
        balancesHolder: BalancesHolder
    ): SingleLimitOrderService {
        return SingleLimitOrderService(
            executionContextFactory,
            genericLimitOrdersProcessor,
            stopOrderBookProcessor,
            executionDataApplyService,
            previousLimitOrdersProcessor,
            balancesHolder
        )
    }

    @Bean
    fun multiLimitOrderService(
        genericLimitOrdersProcessor: GenericLimitOrdersProcessor,
        executionContextFactory: ExecutionContextFactory,
        previousLimitOrdersProcessor: PreviousLimitOrdersProcessor,
        stopOrderBookProcessor: StopOrderBookProcessor,
        executionDataApplyService: ExecutionDataApplyService,
        assetsHolder: AssetsHolder,
        assetsPairsHolder: AssetsPairsHolder,
        balancesHolder: BalancesHolder,
        applicationSettingsHolder: ApplicationSettingsHolder,
        messageProcessingStatusHolder: MessageProcessingStatusHolder
    ): MultiLimitOrderService {
        return MultiLimitOrderService(
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
    }

    @Bean
    fun marketOrderService(
        matchingEngine: MatchingEngine,
        executionContextFactory: ExecutionContextFactory,
        stopOrderBookProcessor: StopOrderBookProcessor,
        executionDataApplyService: ExecutionDataApplyService,
        matchingResultHandlingHelper: MatchingResultHandlingHelper,
        genericLimitOrderService: GenericLimitOrderService,
        assetsPairsHolder: AssetsPairsHolder,
        marketOrderValidator: MarketOrderValidator,
        messageSequenceNumberHolder: MessageSequenceNumberHolder,
        messageSender: MessageSender,
        applicationSettingsHolder: ApplicationSettingsHolder,
        messageProcessingStatusHolder: MessageProcessingStatusHolder,
        balancesHolder: BalancesHolder
    ): MarketOrderService {
        return MarketOrderService(
            matchingEngine,
            executionContextFactory,
            stopOrderBookProcessor,
            executionDataApplyService,
            matchingResultHandlingHelper,
            genericLimitOrderService,
            assetsPairsHolder,
            marketOrderValidator,
            applicationSettingsHolder,
            messageSequenceNumberHolder,
            messageSender,
            messageProcessingStatusHolder,
            balancesHolder
        )
    }

    @Bean
    fun minVolumeOrderCanceller(
        assetsPairsHolder: AssetsPairsHolder,
        genericLimitOrderService: GenericLimitOrderService,
        limitOrdersCancelExecutor: LimitOrdersCancelExecutor
    ): MinVolumeOrderCanceller {
        return MinVolumeOrderCanceller(
            assetsPairsHolder,
            genericLimitOrderService,
            limitOrdersCancelExecutor,
            true
        )
    }

    @Bean
    fun genericStopLimitOrderService(
        stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder,
        expiryOrdersQueue: ExpiryOrdersQueue
    ): GenericStopLimitOrderService {
        return GenericStopLimitOrderService(stopOrdersDatabaseAccessorsHolder, expiryOrdersQueue)
    }

    @Bean
    fun testStopOrderBookDatabaseAccessor(testFileStopOrderDatabaseAccessor: TestFileStopOrderDatabaseAccessor): TestStopOrderBookDatabaseAccessor {
        return TestStopOrderBookDatabaseAccessor(testFileStopOrderDatabaseAccessor)
    }

    @Bean
    fun testFileOrderDatabaseAccessor(): TestFileOrderDatabaseAccessor {
        return TestFileOrderDatabaseAccessor()
    }

    @Bean
    fun testFileStopOrderDatabaseAccessor(): TestFileStopOrderDatabaseAccessor {
        return TestFileStopOrderDatabaseAccessor()
    }

    @Bean
    fun orderBookListener(): TestOrderBookListener {
        return TestOrderBookListener()
    }

    @Bean
    fun testOrderBookWrapper(
        genericLimitOrderService: GenericLimitOrderService,
        testOrderBookDatabaseAccessor: TestOrderBookDatabaseAccessor,
        genericStopLimitOrderService: GenericStopLimitOrderService,
        stopOrderBookDatabaseAccessor: TestStopOrderBookDatabaseAccessor
    ): TestOrderBookWrapper {
        return TestOrderBookWrapper(
            genericLimitOrderService,
            testOrderBookDatabaseAccessor,
            genericStopLimitOrderService,
            stopOrderBookDatabaseAccessor
        )
    }

    @Bean
    fun allOrdersCanceller(
        genericLimitOrderService: GenericLimitOrderService,
        genericStopLimitOrderService: GenericStopLimitOrderService,
        limitOrdersCancelExecutor: LimitOrdersCancelExecutor
    ): AllOrdersCanceller {
        return AllOrdersCanceller(
            genericLimitOrderService,
            genericStopLimitOrderService,
            limitOrdersCancelExecutor,
            true
        )
    }

    @Bean
    fun feeProcessor(
        assetsHolder: AssetsHolder,
        assetsPairsHolder: AssetsPairsHolder,
        genericLimitOrderService: GenericLimitOrderService
    ): FeeProcessor {
        return FeeProcessor(assetsHolder, assetsPairsHolder, genericLimitOrderService)
    }

    @Bean
    fun cashInOutContextParser(assetsHolder: AssetsHolder): CashInOutContextParser {
        return CashInOutContextParser(assetsHolder)
    }

    @Bean
    fun reservedCashInOutContextParser(assetsHolder: AssetsHolder): ReservedCashInOutContextParser {
        return ReservedCashInOutContextParser(assetsHolder)
    }

    @Bean
    fun processedMessagesCache(): ProcessedMessagesCache {
        return Mockito.mock(ProcessedMessagesCache::class.java)
    }

    @Bean
    fun cashInOutPreprocessor(
        cashInOutContextParser: CashInOutContextParser,
        persistenceManager: PersistenceManager,
        processedMessagesCache: ProcessedMessagesCache,
        messageProcessingStatusHolder: MessageProcessingStatusHolder
    ): CashInOutPreprocessor {
        return CashInOutPreprocessor(
            cashInOutContextParser,
            LinkedBlockingQueue(),
            Mockito.mock(CashOperationIdDatabaseAccessor::class.java),
            persistenceManager,
            processedMessagesCache,
            messageProcessingStatusHolder,
            ThrottlingLogger.getLogger("cashInOut")
        )
    }

    @Bean
    fun cashTransferInitializer(assetsHolder: AssetsHolder): CashTransferContextParser {
        return CashTransferContextParser(assetsHolder)
    }

    @Bean
    fun healthMonitor(): HealthMonitor {
        return Mockito.mock(HealthMonitor::class.java) {
            true
        }
    }

    @Bean
    fun messageProcessingStatusHolder(
        generalHealthMonitor: HealthMonitor,
        applicationSettingsHolder: ApplicationSettingsHolder,
        disabledFunctionalityRulesHolder: DisabledFunctionalityRulesHolder
    ): MessageProcessingStatusHolder {
        return MessageProcessingStatusHolder(
            generalHealthMonitor,
            applicationSettingsHolder,
            disabledFunctionalityRulesHolder
        )
    }

    @Bean
    fun cashTransferPreprocessor(
        cashTransferContextParser: CashTransferContextParser,
        persistenceManager: PersistenceManager,
        processedMessagesCache: ProcessedMessagesCache,
        messageProcessingStatusHolder: MessageProcessingStatusHolder
    ): CashTransferPreprocessor {
        return CashTransferPreprocessor(
            cashTransferContextParser,
            LinkedBlockingQueue(),
            Mockito.mock(CashOperationIdDatabaseAccessor::class.java),
            persistenceManager,
            processedMessagesCache,
            messageProcessingStatusHolder,
            ThrottlingLogger.getLogger("transfer")
        )
    }

    @Bean
    fun messageBuilder(
        cashTransferContextParser: CashTransferContextParser,
        cashInOutContextParser: CashInOutContextParser,
        reservedCashInOutContextParser: ReservedCashInOutContextParser,
        singleLimitOrderContextParser: SingleLimitOrderContextParser,
        limitOrderCancelOperationContextParser: ContextParser<LimitOrderCancelOperationParsedData, LimitOrderCancelMessageWrapper>,
        limitOrderMassCancelOperationContextParser: ContextParser<LimitOrderMassCancelOperationParsedData, LimitOrderMassCancelMessageWrapper>
    ): MessageBuilder {
        return MessageBuilder(
            singleLimitOrderContextParser,
            cashInOutContextParser,
            reservedCashInOutContextParser,
            cashTransferContextParser,
            limitOrderCancelOperationContextParser,
            limitOrderMassCancelOperationContextParser
        )
    }

    @Bean
    fun cashTransferOperationService(
        balancesHolder: BalancesHolder,
        feeProcessor: FeeProcessor,
        cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator,
        messageSequenceNumberHolder: MessageSequenceNumberHolder,
        messageSender: MessageSender
    ): CashTransferOperationService {
        return CashTransferOperationService(
            balancesHolder,
            feeProcessor,
            cashTransferOperationBusinessValidator,
            messageSequenceNumberHolder,
            messageSender
        )
    }

    @Bean
    fun settingsListener(): SettingsListener {
        return SettingsListener()
    }

    @Bean
    fun messageProcessingSwitchSettingValidator(): SettingValidator {
        return MessageProcessingSwitchSettingValidator()
    }

    @Bean
    fun settingValidators(settingValidators: List<SettingValidator>): Map<AvailableSettingGroup, List<SettingValidator>> {
        return settingValidators.groupBy { it.getSettingGroup() }
    }

    @Bean
    fun singleLimitOrderContextParser(
        assetsPairsHolder: AssetsPairsHolder, assetsHolder: AssetsHolder,
        applicationSettingsHolder: ApplicationSettingsHolder
    ): SingleLimitOrderContextParser {
        return SingleLimitOrderContextParser(
            assetsPairsHolder,
            assetsHolder,
            applicationSettingsHolder,
            ThrottlingLogger.getLogger("limitOrder")
        )
    }

    @Bean
    fun limitOrderInputValidator(applicationSettingsHolder: ApplicationSettingsHolder): LimitOrderInputValidator {
        return LimitOrderInputValidatorImpl(applicationSettingsHolder)
    }


    @Bean
    fun limitOrderBusinessValidator(): LimitOrderBusinessValidator {
        return LimitOrderBusinessValidatorImpl()
    }

    @Bean
    fun stopOrderBusinessValidatorImpl(): StopOrderBusinessValidatorImpl {
        return StopOrderBusinessValidatorImpl()
    }

    @Bean
    fun limitOrderCancelOperationInputValidator(): LimitOrderCancelOperationInputValidator {
        return LimitOrderCancelOperationInputValidatorImpl()
    }

    @Bean
    fun limitOrderCancelOperationBusinessValidator(): LimitOrderCancelOperationBusinessValidator {
        return LimitOrderCancelOperationBusinessValidatorImpl()
    }

    @Bean
    fun limitOrderCancelService(
        genericLimitOrderService: GenericLimitOrderService,
        genericStopLimitOrderService: GenericStopLimitOrderService,
        limitOrderCancelOperationBusinessValidator: LimitOrderCancelOperationBusinessValidator,
        limitOrdersCancelExecutor: LimitOrdersCancelExecutor
    ): LimitOrderCancelService {
        return LimitOrderCancelService(
            genericLimitOrderService,
            genericStopLimitOrderService,
            limitOrderCancelOperationBusinessValidator,
            limitOrdersCancelExecutor
        )
    }

    @Bean
    fun limitOrderCancelOperationContextParser(): LimitOrderCancelOperationContextParser {
        return LimitOrderCancelOperationContextParser()
    }

    @Bean
    fun limitOrderMassCancelOperationContextParser(): LimitOrderMassCancelOperationContextParser {
        return LimitOrderMassCancelOperationContextParser()
    }

    @Bean
    fun limitOrderMassCancelService(
        genericLimitOrderService: GenericLimitOrderService,
        genericStopLimitOrderService: GenericStopLimitOrderService,
        limitOrdersCancelExecutor: LimitOrdersCancelExecutor
    ): LimitOrderMassCancelService {
        return LimitOrderMassCancelService(
            genericLimitOrderService,
            genericStopLimitOrderService,
            limitOrdersCancelExecutor
        )
    }

    @Bean
    fun multiLimitOrderCancelService(
        genericLimitOrderService: GenericLimitOrderService,
        limitOrdersCancelExecutor: LimitOrdersCancelExecutor,
        applicationSettingsHolder: ApplicationSettingsHolder
    ): MultiLimitOrderCancelService {
        return MultiLimitOrderCancelService(
            genericLimitOrderService,
            limitOrdersCancelExecutor,
            applicationSettingsHolder
        )
    }

    @Bean
    fun disabledFunctionalityRulesService(): DisabledFunctionalityRulesService {
        return DisabledFunctionalityRulesServiceImpl()
    }

    @Bean
    fun singleLimitOrderPreprocessor(
        singleLimitOrderContextParser: SingleLimitOrderContextParser,
        preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
        messageProcessingStatusHolder: MessageProcessingStatusHolder
    ): SingleLimitOrderPreprocessor {
        return SingleLimitOrderPreprocessor(
            singleLimitOrderContextParser,
            preProcessedMessageQueue,
            messageProcessingStatusHolder,
            ThrottlingLogger.getLogger("limitOrder")
        )
    }

    @Bean
    fun expiryOrdersQueue() = ExpiryOrdersQueue()
}