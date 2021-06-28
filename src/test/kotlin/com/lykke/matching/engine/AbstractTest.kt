package com.lykke.matching.engine

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestPersistenceManager
import com.lykke.matching.engine.database.TestStopOrderBookDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.database.cache.AssetPairsCache
import com.lykke.matching.engine.database.cache.AssetsCache
import com.lykke.matching.engine.holders.*
import com.lykke.matching.engine.notification.TestOrderBookListener
import com.lykke.matching.engine.notification.TradeInfoListener
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.services.*
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.order.MinVolumeOrderCanceller
import org.junit.After
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal
import java.util.concurrent.BlockingQueue
import kotlin.test.assertEquals

abstract class AbstractTest {
    @Autowired
    lateinit var balancesHolder: BalancesHolder

    @Autowired
    protected lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    @Autowired
    protected lateinit var ordersDatabaseAccessorsHolder: OrdersDatabaseAccessorsHolder

    @Autowired
    protected lateinit var stopOrdersDatabaseAccessorsHolder: StopOrdersDatabaseAccessorsHolder

    protected lateinit var testWalletDatabaseAccessor: TestWalletDatabaseAccessor
    protected lateinit var stopOrderDatabaseAccessor: TestStopOrderBookDatabaseAccessor

    @Autowired
    private lateinit var assetsCache: AssetsCache

    @Autowired
    protected lateinit var applicationSettingsHolder: ApplicationSettingsHolder

    @Autowired
    protected lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    protected lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    protected lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Autowired
    protected lateinit var assetPairsCache: AssetPairsCache

    @Autowired
    protected lateinit var persistenceManager: TestPersistenceManager

    @Autowired
    protected lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    protected lateinit var singleLimitOrderService: SingleLimitOrderService

    @Autowired
    protected lateinit var multiLimitOrderService: MultiLimitOrderService

    @Autowired
    protected lateinit var marketOrderService: MarketOrderService

    @Autowired
    protected lateinit var minVolumeOrderCanceller: MinVolumeOrderCanceller

    @Autowired
    protected lateinit var testOrderBookListener: TestOrderBookListener

    @Autowired
    protected lateinit var genericStopLimitOrderService: GenericStopLimitOrderService

    @Autowired
    protected lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Autowired
    protected lateinit var tradesInfoListener: TradeInfoListener

    @Autowired
    protected lateinit var limitOrderCancelService: LimitOrderCancelService

    @Autowired
    protected lateinit var cashTransferOperationsService: CashTransferOperationService

    @Autowired
    protected lateinit var clientsEventsQueue: BlockingQueue<Event<*>>

    @Autowired
    protected lateinit var trustedClientsEventsQueue: BlockingQueue<ExecutionEvent>

    @Autowired
    protected lateinit var limitOrderMassCancelService: LimitOrderMassCancelService

    @Autowired
    protected lateinit var cashInOutOperationService: CashInOutOperationService

    @Autowired
    protected lateinit var multiLimitOrderCancelService: MultiLimitOrderCancelService

    protected open fun initServices() {
        testWalletDatabaseAccessor = balancesDatabaseAccessorsHolder.primaryAccessor as TestWalletDatabaseAccessor
        stopOrderDatabaseAccessor =
            stopOrdersDatabaseAccessorsHolder.primaryAccessor as TestStopOrderBookDatabaseAccessor
        clearMessageQueues()
        assetsCache.update()
        assetPairsCache.update()
        applicationSettingsCache.update()
        applicationSettingsHolder.update()
    }

    protected fun clearMessageQueues() {
        tradesInfoListener.clear()

        clientsEventsQueue.clear()
        trustedClientsEventsQueue.clear()
    }

    protected fun assertOrderBookSize(assetPairId: String, isBuySide: Boolean, size: Int) {
        assertEquals(size, genericLimitOrderService.getOrderBook("", assetPairId).getOrderBook(isBuySide).size)

        // check cache orders map size
        val allClientIds = testWalletDatabaseAccessor.loadWallets().keys
        assertEquals(
            size,
            allClientIds.sumBy { genericLimitOrderService.searchOrders(it, assetPairId, isBuySide).size })
    }

    protected fun assertStopOrderBookSize(assetPairId: String, isBuySide: Boolean, size: Int) {
        assertEquals(size, genericStopLimitOrderService.getOrderBook("", assetPairId).getOrderBook(isBuySide).size)

        // check cache orders map size
        val allClientIds = testWalletDatabaseAccessor.loadWallets().keys
        assertEquals(
            size,
            allClientIds.sumBy { genericStopLimitOrderService.searchOrders(it, assetPairId, isBuySide).size })
    }

    protected fun assertBalance(clientId: String, assetId: String, balance: Double? = null, reserved: Double? = null) {
        if (balance != null) {
            assertEquals(BigDecimal.valueOf(balance), balancesHolder.getBalance(clientId, assetId))
            assertEquals(BigDecimal.valueOf(balance), testWalletDatabaseAccessor.getBalance(clientId, assetId))
        }
        if (reserved != null) {
            assertEquals(
                BigDecimal.valueOf(reserved),
                balancesHolder.getReservedForOrdersBalance("", "", clientId, assetId)
            )
            assertEquals(BigDecimal.valueOf(reserved), testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
        }
    }

    @After
    open fun tearDown() {
    }

    protected fun assertEventBalanceUpdate(
        clientId: String,
        assetId: String,
        oldBalance: String?,
        newBalance: String?,
        oldReserved: String?,
        newReserved: String?,
        balanceUpdates: Collection<BalanceUpdate>
    ) {
        val balanceUpdate = balanceUpdates.single { it.walletId == clientId && it.assetId == assetId }
        assertEquals(oldBalance, balanceUpdate.oldBalance)
        assertEquals(newBalance, balanceUpdate.newBalance)
        assertEquals(oldReserved, balanceUpdate.oldReserved)
        assertEquals(newReserved, balanceUpdate.newReserved)
    }
}