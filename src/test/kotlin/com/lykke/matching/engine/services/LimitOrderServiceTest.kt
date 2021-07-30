package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderType
import com.lykke.matching.engine.outgoing.messages.v2.enums.TradeRole
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrderFeeInstructions
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.createAssetPair
import com.lykke.matching.engine.utils.getSetting
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderServiceTest : AbstractTest() {
    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()

            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "ETH", 6))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))

            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        fun testConfig(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(
                AvailableSettingGroup.TRUSTED_CLIENTS,
                getSetting("Client3")
            )
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Autowired
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)

        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURCHF", "EUR", "CHF", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCEUR", "BTC", "EUR", 8))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "ETHBTC", "ETH", "BTC", 5))

        initServices()
    }

    @Test
    fun testNotEnoughFundsClientOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 500.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.2,
                    volume = -501.0
                )
            )
        )
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders[0].status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, event.orders[0].rejectReason)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.2,
                    volume = -501.0
                ), true
            )
        )

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders[0].status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, event.orders[0].rejectReason)
    }

    @Test
    fun testTrustedClientNotEnoughFundsBuyClientInOrderBook() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 900.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client3",
                    price = 1.1,
                    volume = -1000.0
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    price = 1.2,
                    volume = 2000.0
                )
            )
        )
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.first().status)
    }

    @Test
    fun testTrustedClientNotEnoughFundsPartialMatched() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 3000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 900.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client3",
                    price = 1.1,
                    volume = -1000.0
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    price = 1.2,
                    volume = 100.0
                )
            )
        )

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("-900", event.orders[1].remainingVolume)
    }

    @Test
    fun testNotEnoughFundsClientSellOrderWithCancel() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 500.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.2, volume = -500.0, uid = "forCancel"))

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.2,
                    volume = -1001.0,
                    uid = "NotEnoughFunds"
                ), true
            )
        )

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "forCancel" }.status)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single { it.externalId == "NotEnoughFunds" }.status)
        assertEquals(
            OrderRejectReason.NOT_ENOUGH_FUNDS,
            event.orders.single { it.externalId == "NotEnoughFunds" }.rejectReason
        )
        assertEquals(1, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "EUR", "1000", "1000", "500", "0", event.balanceUpdates!!)
    }

    @Test
    fun testLeadToNegativeSpreadForClientOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 500.0)
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.25, volume = 10.0))
        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.2,
                    volume = -500.0
                )
            )
        )

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.LEAD_TO_NEGATIVE_SPREAD, event.orders.single().rejectReason)
        assertEquals(0, event.balanceUpdates?.size)
    }

    @Test
    fun testAddLimitOrder() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 999.9,
                    volume = 1.0
                )
            )
        )

        val order = ordersDatabaseAccessorsHolder.primaryAccessor.loadLimitOrders()
            .find { NumberUtils.equalsIgnoreScale(it.price, BigDecimal.valueOf(999.9)) }
        assertNotNull(order)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(999.9), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.single().status)
        assertEquals(1, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "USD", "1000", "1000", "0", "999.9", event.balanceUpdates!!)
    }

    @Test
    fun testAddSellLimitOrder() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 999.9,
                    volume = -1.0
                )
            )
        )

        val order = ordersDatabaseAccessorsHolder.primaryAccessor.loadLimitOrders()
            .find { NumberUtils.equalsIgnoreScale(it.price, BigDecimal.valueOf(999.9)) }
        assertNotNull(order)

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.single().status)
        assertEquals(1, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "EUR", "1000", "1000", "0", "1", event.balanceUpdates!!)
    }

    @Test
    fun testCancelPrevAndAddLimitOrder() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 100.0,
                    volume = 1.0,
                    uid = "1"
                )
            )
        )
        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 200.0,
                    volume = 1.0,
                    uid = "2"
                )
            )
        )
        assertEquals(BigDecimal.valueOf(300.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 300.0,
                    volume = 2.0,
                    uid = "3"
                ), true
            )
        )
        assertEquals(BigDecimal.valueOf(600.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        var order = ordersDatabaseAccessorsHolder.primaryAccessor.loadLimitOrders()
            .find { NumberUtils.equalsIgnoreScale(it.price, BigDecimal.valueOf(300.0)) }
        assertNotNull(order)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 500.0,
                    volume = 1.5,
                    uid = "3"
                ), true
            )
        )
        assertEquals(BigDecimal.valueOf(750.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        order = ordersDatabaseAccessorsHolder.primaryAccessor.loadLimitOrders()
            .find { NumberUtils.equalsIgnoreScale(it.price, BigDecimal.valueOf(500.0)) }
        assertNotNull(order)
    }

    @Test
    fun testRestartAndCancel() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 100.0,
                    volume = 1.0,
                    uid = "1"
                )
            )
        )
        assertEquals(BigDecimal.valueOf(100.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 200.0,
                    volume = 1.0,
                    uid = "2"
                )
            )
        )
        assertEquals(BigDecimal.valueOf(300.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        initServices()

        assertNotNull(genericLimitOrderService.searchOrders("Client1", "EURUSD", true).find { it.externalId == "2" })
        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("2"))
    }

    @Test
    fun testNegativeSpread() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 100.0,
                    volume = 1.0
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 200.0,
                    volume = 1.0
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 300.0,
                    volume = -1.0
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 150.0,
                    volume = -1.0
                )
            )
        )
    }

    @Test
    fun testSmallVolume() {
        testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
        testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))
        testDictionariesDatabaseAccessor.addAssetPair(
            createAssetPair(
                "", "EURUSD", "EUR", "USD", 5,
                BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.2)
            )
        )

        initServices()

        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(buildLimitOrder(volume = 0.09)))

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.TOO_SMALL_VOLUME, event.orders.single().rejectReason)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.9,
                    volume = 0.1
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertTrue(OutgoingOrderStatus.REJECTED != event.orders.single().status)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 2.0,
                    volume = -0.1
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertTrue(OutgoingOrderStatus.REJECTED != event.orders.single().status)
    }

    @Test
    fun testAddAndMatchLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 122.512,
                volume = -10.0,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 122.524,
                volume = -10.0,
                clientId = "Client3"
            )
        )

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 122.52,
                    volume = 1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals(4, event.balanceUpdates?.size)

        assertEventBalanceUpdate("Client1", "USD", "1000", "877.48", "0", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client1", "EUR", "1000", "1001", "0", "0", event.balanceUpdates!!)
        val eventOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(1, eventOrder.trades?.size)
        val eventTrade = eventOrder.trades!!.single()
        assertEquals(eventTrade.baseVolume, "1")
        assertEquals(eventTrade.baseAssetId, "EUR")
        assertEquals(eventTrade.oppositeWalletId, "Client3")
        assertEquals("USD", eventTrade.quotingAssetId)
        assertEquals("-122.52", eventTrade.quotingVolume)
        assertEquals(eventTrade.price, "122.512")
        assertEquals(TradeRole.TAKER, eventTrade.role)

        assertEquals(BigDecimal.valueOf(877.48), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderRounding() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    price = 4199.351,
                    volume = 0.00357198
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.single().status)

        assertEquals(BigDecimal.valueOf(15.0), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client3",
                    assetId = "BTCUSD",
                    price = 4199.351,
                    volume = -0.00357198
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testStatusDate() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1.0)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    uid = "order1",
                    assetId = "EURUSD",
                    volume = -1.0,
                    price = 1.0
                )
            )
        )

        assertEquals(OrderStatus.InOrderBook.name, genericLimitOrderService.getOrder("order1")!!.status)
        val inOrderBookStatusDate = genericLimitOrderService.getOrder("order1")!!.statusDate

        Thread.sleep(10)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "EURUSD",
                    volume = 0.4,
                    price = 1.0
                )
            )
        )

        assertEquals(OrderStatus.Processing.name, genericLimitOrderService.getOrder("order1")!!.status)
        val partiallyMatchedStatusDate1 = genericLimitOrderService.getOrder("order1")!!.statusDate

        Thread.sleep(10)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "EURUSD",
                    volume = 0.4,
                    price = 1.0
                )
            )
        )

        assertEquals(OrderStatus.Processing.name, genericLimitOrderService.getOrder("order1")!!.status)
        val partiallyMatchedStatusDate2 = genericLimitOrderService.getOrder("order1")!!.statusDate

        assertTrue(partiallyMatchedStatusDate1!! > inOrderBookStatusDate)
        assertEquals(partiallyMatchedStatusDate1, partiallyMatchedStatusDate2)
    }

    @Test
    fun testAddLimitOrderEURUSD() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    price = 325.7152,
                    volume = 0.046053
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        assertEquals(BigDecimal.valueOf(15.01), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    clientId = "Client1",
                    price = 3200.0,
                    volume = -0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    clientId = "Client3",
                    price = 3200.0,
                    volume = 0.009973
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(1000 - 0.009973), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(1031.92), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(0.009973), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(968.08), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithDust1() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    clientId = "Client1",
                    price = 3200.0,
                    volume = -0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    clientId = "Client3",
                    price = 3200.0,
                    volume = 0.01002635
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("0.00002635", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(999.99), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(1032.0), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(0.01), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(968.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
    }

    @Test
    fun testAddAndMatchBuyLimitOrderWithDust() {
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    clientId = "Client1",
                    price = 3200.0,
                    volume = 0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    clientId = "Client3",
                    price = 3200.0,
                    volume = -0.009973
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(1000 - 0.009973), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(31.91), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(0.009973), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(968.09), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(0.09), testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
    }

    @Test
    fun testAddAndMatchLimitOrderWithSamePrice() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 122.512,
                volume = -10.0,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 122.524,
                volume = -10.0,
                clientId = "Client3"
            )
        )

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 122.512,
                    volume = 1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(877.48), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchLimitSellDustOrder() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1000.0)
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3583.081,
                volume = 0.00746488,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3581.391,
                volume = 0.00253512,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3579.183,
                volume = 0.00253512,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3578.183,
                volume = 0.00253512,
                clientId = "Client3"
            )
        )

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCEUR",
                    price = 3575.782,
                    volume = -0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)
        assertEquals("0", event.orders[0].remainingVolume)

        assertEquals(BigDecimal.valueOf(1035.81), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(999.99), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

    @Test
    fun testAddAndMatchBuyLimitDustOrder() {
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 4000.0)
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3827.395,
                volume = -0.00703833,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3830.926,
                volume = -0.01356452,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3832.433,
                volume = -0.02174805,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3836.76,
                volume = -0.02740016,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3838.624,
                volume = -0.03649953,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3842.751,
                volume = -0.03705699,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3845.948,
                volume = -0.04872587,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3847.942,
                volume = -0.05056858,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3851.385,
                volume = -0.05842735,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3855.364,
                volume = -0.07678406,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3858.021,
                volume = -0.07206853,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3861.283,
                volume = -0.05011803,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 3863.035,
                volume = -0.1,
                clientId = "Client3"
            )
        )

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCEUR",
                    price = 3890.0,
                    volume = 0.5
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(13, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[3].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[4].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[5].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[6].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[7].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[8].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[9].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[10].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[11].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[12].status)
        assertEquals("0", event.orders[0].remainingVolume)
        assertEquals(12, event.orders[0].trades?.size)
        assertEquals(1, event.orders[1].trades?.size)
        assertEquals(1, event.orders[12].trades?.size)
        assertEquals(4, event.balanceUpdates?.size)

        assertEquals(BigDecimal.valueOf(2075.46), testWalletDatabaseAccessor.getBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(0.5), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        assertEquals(BigDecimal.valueOf(1924.54), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(999.5), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client3", "BTC"))
    }

    @Test
    fun testAddAndPartiallyMatchLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 122.512,
                volume = -10.0,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 122.524,
                volume = -10.0,
                clientId = "Client3"
            )
        )

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 122.52,
                    volume = 11.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("1", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(774.88), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(122.52), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddMatchWithMarketOrder() {
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 122.52,
                    volume = 11.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)
        assertEquals("11", event.orders[0].remainingVolume)

        assertEquals(BigDecimal.valueOf(2000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(1347.72), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client3",
                    assetId = "EURUSD",
                    volume = -10.0
                )
            )
        )

        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("1", event.orders[0].remainingVolume)

        assertEquals(BigDecimal.valueOf(774.8), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(122.52), testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testAddAndMatchWithLimitOrder() {
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 406.24)
        testBalanceHolderWrapper.updateReservedBalance("Client4", "USD", 263.33)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 4421.0,
                    volume = -0.00045239
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)
        assertEquals("-0.00045239", event.orders[0].remainingVolume)

        assertEquals(BigDecimal.valueOf(2000.0), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.00045239), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client3", "USD"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    price = 4425.0,
                    volume = 0.032
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("0.03154761", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("0", event.orders[1].remainingVolume)

        assertEquals(BigDecimal.valueOf(1999.99954761), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(2000.00045239), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(
            NumberUtils.setScale(BigDecimal.valueOf(263.33 + 139.59), 2, true),
            testWalletDatabaseAccessor.getReservedBalance("Client4", "USD")
        )
    }

    @Test
    fun testAddAndMatchAndCancel() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.00148747)
        testBalanceHolderWrapper.updateBalance("Client2", "ETH", 1000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "ETHBTC",
                    uid = "1",
                    price = 0.07963,
                    volume = 2.244418
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)

        assertEquals(BigDecimal.valueOf(1.00148747), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.17872301), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "ETHBTC",
                    uid = "2",
                    price = 0.07948,
                    volume = -0.01462
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(0.17755882), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "ETHBTC",
                    uid = "3",
                    price = 0.07954,
                    volume = -0.031344
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(0.1750629), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "ETHBTC",
                    uid = "4",
                    price = 0.07958,
                    volume = -0.041938
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(0.99448784), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.17172338), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "ETHBTC",
                    uid = "5",
                    price = 0.07948,
                    volume = -0.000001
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[1].status)
        assertEquals("2.156515", event.orders[1].remainingVolume)

        assertEquals(BigDecimal.valueOf(0.99448777), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.valueOf(0.17172331), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))

        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("1"))

        assertEquals(BigDecimal.valueOf(0.99448777), testWalletDatabaseAccessor.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
    }

    @Test
    fun testAddAndMatchWithLimitOrder1() {
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 23.4)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 2000.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 2000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    price = 4680.0,
                    volume = 0.005
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)
        assertEquals("0.005", event.orders[0].remainingVolume)

        assertEquals(BigDecimal.valueOf(23.4), testWalletDatabaseAccessor.getReservedBalance("Client4", "USD"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    price = 4650.0,
                    volume = 0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PLACED, event.orders[0].status)
        assertEquals("0.01", event.orders[0].remainingVolume)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 4600.0,
                    volume = -0.005
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals("4680", event.orders[1].price)
    }

    @Test
    fun testAddAndMatchWithLimitOrder2() {
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 110.0)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 2000.0)

        initServices()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCEUR",
                    price = 3571.922,
                    volume = -0.00662454
                )
            )
        )
        clearMessageQueues()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client4",
                    assetId = "BTCEUR",
                    price = 3571.922,
                    volume = 0.03079574
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[0].status)
        assertEquals("0.0241712", event.orders[0].remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.valueOf(86.33), testWalletDatabaseAccessor.getReservedBalance("Client4", "EUR"))
    }

    @Test
    fun testMatchWithOwnLimitOrder() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.0, volume = -10.0))
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 10.00)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 10.00)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10.00)


        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.0,
                    volume = 10.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders[0].status)
        assertEquals(OrderRejectReason.LEAD_TO_NEGATIVE_SPREAD, event.orders[0].rejectReason)

        val cacheOrderBook = genericLimitOrderService.getOrderBook("", "EURUSD")
        assertEquals(1, cacheOrderBook.getOrderBook(false).size)
        assertEquals(BigDecimal.valueOf(1.0), cacheOrderBook.getAskPrice())


        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    price = 1.0,
                    volume = 10.0,
                    clientId = "Client2"
                )
            )
        )

        assertEquals(0, genericLimitOrderService.getOrderBook("", "EURUSD").getOrderBook(false).size)
    }

    @Test
    fun testMatchMarketOrderWithOwnLimitOrder() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.0, volume = -10.0))
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 10.00)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 11.00)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 10.00)

        initServices()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = 10.0)))

        assertEquals(1, clientsEventsQueue.size)
        var event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.NO_LIQUIDITY, event.orders.single().rejectReason)
        assertEquals(0, event.orders.single().trades?.size)
        assertEquals(OrderType.MARKET, event.orders.single().orderType)

        assertEquals(1, genericLimitOrderService.getOrderBook("", "EURUSD").getOrderBook(false).size)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    price = 1.1,
                    volume = -10.0
                )
            )
        )

        assertEquals(2, genericLimitOrderService.getOrderBook("", "EURUSD").getOrderBook(false).size)

        clearMessageQueues()

        marketOrderService.processMessage(buildMarketOrderWrapper(buildMarketOrder(volume = 10.0)))

        assertEquals(1, clientsEventsQueue.size)
        event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("1.1", eventMarketOrder.price)
        assertEquals(1, eventMarketOrder.trades?.size)

        val cacheOrderBook = genericLimitOrderService.getOrderBook("", "EURUSD")
        assertEquals(1, cacheOrderBook.getOrderBook(false).size)
        assertEquals(BigDecimal.valueOf(1.0), cacheOrderBook.getAskPrice())
    }

    @Test
    fun testMatchWithLimitOrderForAllFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 700.04)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 700.04)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 2.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                price = 4722.0,
                volume = 0.14825226
            )
        )
        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    clientId = "Client2",
                    price = 4721.403,
                    volume = -0.4435
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedForOrdersBalance("", "", "Client1", "USD"))
        assertEquals(0, genericLimitOrderService.getOrderBook("", "BTCUSD").getOrderBook(true).size)
    }

    @Test
    fun testMatchWithSeveralOrdersOfSameClient() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 29.99)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 100.00)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "BTC", 0.0)

        testOrderBookWrapper.addLimitOrder(buildLimitOrder(assetId = "BTCUSD", volume = -29.98, price = 6100.0))
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                uid = "limit-order-1",
                assetId = "BTCUSD",
                volume = -0.01,
                price = 6105.0
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client3",
                assetId = "BTCUSD",
                volume = -0.1,
                price = 6110.0
            )
        )

        initServices()

        assertEquals(BigDecimal.valueOf(29.99), balancesHolder.getReservedForOrdersBalance("", "", "Client1", "BTC"))

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    volume = 30.0,
                    price = 6110.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(4, event.orders.size)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[0].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[1].status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders[2].status)
        assertEquals("limit-order-1", event.orders[2].externalId)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders[3].status)
        assertEquals("-0.09", event.orders[3].remainingVolume)

        assertEquals(BigDecimal.valueOf(70.01), balancesHolder.getBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedForOrdersBalance("", "", "Client1", "BTC"))
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder1() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        val order = buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = 1.0)
        order.reservedLimitVolume = BigDecimal.valueOf(1.19)
        testOrderBookWrapper.addLimitOrder(order)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "EURUSD",
                    price = 1.1,
                    volume = -2.0,
                    clientId = "Client2"
                )
            )
        )

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventCancelledOrder = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }
        assertEquals(1, eventCancelledOrder.size)
        assertEquals("Client1", eventCancelledOrder.first().walletId)
        val eventBalanceUpdate = event.balanceUpdates?.singleOrNull { it.walletId == "Client1" }
        assertNotNull(eventBalanceUpdate)
        assertEquals("0", eventBalanceUpdate.newReserved)
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder2() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 1.2,
                volume = 1.0,
                clientId = "Client1",
                reservedVolume = 1.19
            )
        )

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "EURUSD",
                    price = 1.1,
                    volume = -2.0,
                    clientId = "Client2"
                )
            )
        )

        assertEquals(BigDecimal.valueOf(1000.0), testWalletDatabaseAccessor.getBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder3() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1.19)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURUSD",
                price = 1.2,
                volume = 1.0,
                clientId = "Client1",
                reservedVolume = 1.19
            )
        )

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "EURUSD",
                    price = 1.1,
                    volume = -2.0,
                    clientId = "Client2"
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(0, event.balanceUpdates!!.filter { it.walletId == "Client1" }.size)
    }

    @Test
    fun testMatchWithNotEnoughFundsOrder4() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 1.19)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client4", "USD", 1.14)
        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 1000.0)

        val order1 = buildLimitOrder(clientId = "Client1", assetId = "EURUSD", price = 1.2, volume = 1.0)
        order1.reservedLimitVolume = BigDecimal.valueOf(1.19)
        testOrderBookWrapper.addLimitOrder(order1)

        val order2 = buildLimitOrder(clientId = "Client4", assetId = "EURUSD", price = 1.15, volume = 1.0)
        order2.reservedLimitVolume = BigDecimal.valueOf(1.14)
        testOrderBookWrapper.addLimitOrder(order2)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "EURUSD",
                    price = 1.1,
                    volume = -2.0,
                    clientId = "Client2"
                )
            )
        )

        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client4", "USD"))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventCancelledOrders = event.orders.filter { it.status == OutgoingOrderStatus.CANCELLED }
        assertEquals(2, eventCancelledOrders.size)
        assertEquals(2, event.balanceUpdates!!.filter { it.newReserved == "0" }.size)
    }

    @Test
    fun testMatchSellMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 50.00)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.01000199)
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 49.99)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD", price = 5000.0, volume = 0.01, clientId = "Client1",
                    fees = listOf(
                        NewLimitOrderFeeInstruction(
                            FeeType.CLIENT_FEE,
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.01),
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.01),
                            null,
                            "targetFeeClient",
                            emptyList(),
                            null
                        )
                    )
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD", price = 4999.0, volume = 0.01, clientId = "Client3",
                    fees = listOf(
                        NewLimitOrderFeeInstruction(
                            FeeType.CLIENT_FEE,
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.01),
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.01),
                            null,
                            "targetFeeClient",
                            emptyList(),
                            null
                        )
                    )
                )
            )
        )

        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2", assetId = "BTCUSD", price = 4998.0, volume = -0.01000199,
                    fees = listOf(
                        NewLimitOrderFeeInstruction(
                            FeeType.CLIENT_FEE,
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.01),
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.01),
                            null,
                            "targetFeeClient",
                            emptyList(),
                            null
                        )
                    )
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        val order = event.orders.single { it.walletId == "Client2" }
        assertEquals(OutgoingOrderStatus.CANCELLED, order.status)
        assertEquals(1, order.trades?.size)
        assertEquals("BTC", order.trades!![0].baseAssetId)
        assertEquals("-0.01", order.trades!![0].baseVolume)
        assertEquals("USD", order.trades!![0].quotingAssetId)
        assertEquals("50", order.trades!![0].quotingVolume)
        assertEquals("-0.00000199", order.remainingVolume)

        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.walletId == "Client1" }.status)

        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("BTCUSD", false, 0)

        assertBalance("Client2", "BTC", reserved = 0.0)
    }

    @Test
    fun testMatchSellWithOppositeMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 50.01)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.02)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = 0.01000199,
                    clientId = "Client1"
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = -0.01
                )
            )
        )
        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = -0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)

        val oppositeOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(OutgoingOrderStatus.CANCELLED, oppositeOrder.status)
        assertEquals(0, oppositeOrder.trades?.size)

        val order = event.orders.single { it.walletId == "Client2" }
        assertEquals(OutgoingOrderStatus.PLACED, order.status)
        assertEquals(0, order.trades?.size)

        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("BTCUSD", false, 1)

        assertBalance("Client1", "USD", reserved = 0.0)
        assertBalance("Client2", "BTC", reserved = 0.01)
    }

    @Test
    fun testMatchBuyMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 0.01)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 50.03)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.01)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = -0.01,
                    clientId = "Client1"
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5001.0,
                    volume = -0.01,
                    clientId = "Client3"
                )
            )
        )

        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 5002.0,
                    volume = 0.01000199
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(2, event.orders.first().trades?.size)
        assertEquals("BTC", event.orders.first().trades!![1].baseAssetId)
        assertEquals("0.00000199", event.orders.first().trades!![1].baseVolume)
        assertEquals("USD", event.orders.first().trades!![1].quotingAssetId)
        assertEquals("-0.01", event.orders.first().trades!![1].quotingVolume)
    }

    @Test
    fun testMatchBuyWithOppositeMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 0.01000199)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 100.0)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = -0.01000199
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = 0.01
                )
            )
        )

        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = 0.01
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(1, event.orders.first().trades?.size)
        assertEquals("BTC", event.orders.first().trades!![0].baseAssetId)
        assertEquals("0.00000199", event.orders.first().trades!![0].baseVolume)
        assertEquals("USD", event.orders.first().trades!![0].quotingAssetId)
        assertEquals("-0.01", event.orders.first().trades!![0].quotingVolume)
    }

    @Test
    fun testMatchNotStraightSellMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 100.0)
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 0.010002)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = 0.01,
                    clientId = "Client1"
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = 0.01,
                    clientId = "Client1"
                )
            )
        )

        clearMessageQueues()
        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    volume = 50.01,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(2, eventMarketOrder.trades?.size)
        assertEquals("-0.01", eventMarketOrder.trades!![0].baseVolume)
        assertEquals("50", eventMarketOrder.trades!![0].quotingVolume)
        assertEquals("-0.000002", eventMarketOrder.trades!![1].baseVolume)
        assertEquals("0.01", eventMarketOrder.trades!![1].quotingVolume)
    }

    @Test
    fun testMatchNotStraightBuyMinRemaining() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 0.02)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 50.01)

        testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 5))

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = -0.01,
                    clientId = "Client1"
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    assetId = "BTCUSD",
                    price = 5000.0,
                    volume = -0.01,
                    clientId = "Client1"
                )
            )
        )

        clearMessageQueues()
        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    volume = -50.01,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.REJECTED, eventMarketOrder.status)
        assertEquals(OrderRejectReason.INVALID_VOLUME_ACCURACY, eventMarketOrder.rejectReason)
        assertEquals(0, eventMarketOrder.trades?.size)

        assertOrderBookSize("BTCUSD", false, 2)

        assertBalance("Client1", "USD", reserved = 0.0)
        assertBalance("Client2", "USD", 50.01, 0.0)
    }

    @Test
    fun testOverflowedRemainingVolume() {
        testDictionariesDatabaseAccessor.addAsset(Asset("", "PKT", 12))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "PKTETH", "PKT", "ETH", 5))
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "PKT", 3.0)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "PKTETH",
                    price = 0.0001,
                    volume = -2.689999999998
                )
            )
        )
        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "PKTETH",
                    price = 0.0001,
                    volume = 100.0
                )
            )
        )

        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventOrder = event.orders.single { it.walletId == "Client2" }
        assertEquals("0", eventOrder.remainingVolume)
        assertEquals(OutgoingOrderStatus.MATCHED, eventOrder.status)
    }

    @Test
    fun testReservedBalanceAfterMatching() {
        val client = "Client"

        testBalanceHolderWrapper.updateBalance(client, "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance(client, "BTC", reservedBalance = 0.0)
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 0.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 200.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "USD", 0.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                volume = -0.00952774,
                price = 10495.66
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                volume = -0.1,
                price = 10590.00
            )
        )

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "order1",
                    clientId = client,
                    assetId = "BTCUSD",
                    volume = -0.00947867,
                    price = 10550.0
                )
            )
        )

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client2", assetId = "BTCUSD", volume = -100.0, straight = false
                )
            )
        )

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client2", assetId = "BTCUSD", volume = -100.0, straight = false
                )
            )
        )
        assertBalance(client, "BTC", reserved = 0.0)
    }

    @Test
    fun testLimitOrderSellFullBalance() {
        testDictionariesDatabaseAccessor.addAsset(Asset("", "LKK1Y", 2))
        testDictionariesDatabaseAccessor.addAsset(Asset("", "LKK", 2))

        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "LKK1YLKK", "LKK1Y", "LKK", 4))

        testBalanceHolderWrapper.updateBalance("Client1", "LKK1Y", 5495.03)
        testBalanceHolderWrapper.updateBalance("Client2", "LKK", 10000.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "LKK1YLKK",
                volume = 4.97,
                price = 1.0105
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "LKK1YLKK",
                volume = 5500.0,
                price = 1.0085
            )
        )

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "LKK1YLKK",
                    volume = -5495.03,
                    price = 1.0082,
                    fees = buildLimitOrderFeeInstructions(takerSize = 0.0009, targetClientId = "Client5")
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(
            OutgoingOrderStatus.MATCHED,
            (clientsEventsQueue.first() as ExecutionEvent).orders.single { it.walletId == "Client1" }.status
        )
    }

    @Test
    fun testImmutableReportOrder1() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "order1", clientId = "Client1", assetId = "EURUSD", volume = -1.0, price = 1.0
                )
            )
        )
        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper(listOf("order1")))

        assertEquals(2, clientsEventsQueue.size)
        val event1 = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event1.orders.size)
        assertEquals(OutgoingOrderStatus.PLACED, event1.orders.first().status)
        val event2 = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event2.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event2.orders.first().status)
    }

    @Test
    fun testImmutableReportOrder2() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "order1", clientId = "Client1", assetId = "EURUSD", volume = -1.0, price = 1.0
                )
            )
        )
        clearMessageQueues()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2", assetId = "EURUSD", volume = 0.1, price = 1.0
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2", assetId = "EURUSD", volume = 0.2, price = 1.0
                )
            )
        )

        assertEquals(2, clientsEventsQueue.size)
        val event1 = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event1.orders.size)
        val eventOrder1 = event1.orders.single { it.externalId == "order1" }
        assertEquals("-0.9", eventOrder1.remainingVolume)
        val event2 = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event2.orders.size)
        val eventOrder2 = event2.orders.single { it.externalId == "order1" }
        assertEquals("-0.7", eventOrder2.remainingVolume)
    }

    @Test
    fun testOrderMaxValue() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testDictionariesDatabaseAccessor.addAssetPair(
            createAssetPair(
                "", "BTCUSD", "BTC", "USD", 8,
                maxValue = BigDecimal.valueOf(10000.0)
            )
        )
        assetPairsCache.update()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    volume = -1.1,
                    price = 10000.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VALUE, eventOrder.rejectReason)

        assertOrderBookSize("BTCUSD", false, 0)
    }

    @Test
    fun testOrderMaxVolume() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.1)
        testDictionariesDatabaseAccessor.addAssetPair(
            createAssetPair(
                "", "BTCUSD", "BTC", "USD", 8,
                maxVolume = BigDecimal.valueOf(1.0)
            )
        )
        assetPairsCache.update()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    volume = -1.1,
                    price = 10000.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        val eventOrder = event.orders.single()
        assertEquals(OutgoingOrderStatus.REJECTED, eventOrder.status)
        assertEquals(OrderRejectReason.INVALID_VOLUME, eventOrder.rejectReason)

        assertOrderBookSize("BTCUSD", false, 0)
    }

    @Test
    fun testBuyPriceDeviationThreshold() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 0.6)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 11000.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                price = 10000.0,
                volume = -0.3
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                price = 11000.0,
                volume = -0.3
            )
        )

        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD,
            "BTCUSD",
            "0.09",
            true
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 11000.0,
                    volume = 1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, executionEvent.orders.single().status)
        assertEquals(OrderRejectReason.TOO_HIGH_PRICE_DEVIATION, executionEvent.orders.single().rejectReason)

        assertOrderBookSize("BTCUSD", false, 2)
        assertOrderBookSize("BTCUSD", true, 0)

        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD,
            "BTCUSD",
            "0.1",
            true
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 11000.0,
                    volume = 1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, executionEvent.orders.size)
        assertOrderBookSize("BTCUSD", false, 0)
        assertOrderBookSize("BTCUSD", true, 1)
    }

    @Test
    fun testSellPriceDeviationThreshold() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 10000.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "BTCUSD",
                price = 13000.0,
                volume = 0.3
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "BTCUSD",
                price = 12000.0,
                volume = 0.3
            )
        )

        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD,
            "BTCUSD",
            "0.07",
            true
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    price = 12000.0,
                    volume = -1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        var executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, executionEvent.orders.single().status)
        assertEquals(OrderRejectReason.TOO_HIGH_PRICE_DEVIATION, executionEvent.orders.single().rejectReason)

        assertOrderBookSize("BTCUSD", true, 2)
        assertOrderBookSize("BTCUSD", false, 0)

        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.LO_PRICE_DEVIATION_THRESHOLD,
            "BTCUSD",
            "0.08",
            true
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    price = 12000.0,
                    volume = -1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, executionEvent.orders.size)
        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("BTCUSD", false, 1)
    }

    @Test
    fun testCancelLimitOrdersAfterRejectedIncomingOrder() {
        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 100.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 100.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 60.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1", assetId = "EURUSD", volume = -50.0, price = 1.1,
                // 'not enough funds' fee to cancel this order during matching
                fees = listOf(
                    NewLimitOrderFeeInstruction(
                        FeeType.CLIENT_FEE,
                        null,
                        null,
                        FeeSizeType.PERCENTAGE,
                        BigDecimal.valueOf(0.1),
                        null,
                        "FeeTargetClient",
                        listOf("BTC"),
                        null
                    )
                )
            )
        )

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "EURUSD",
                volume = -50.0,
                price = 1.2
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2", assetId = "EURUSD", volume = 50.0, price = 1.2,
                    // 'not enough funds' fee to cancel this order after matching
                    fees = listOf(
                        NewLimitOrderFeeInstruction(
                            FeeType.CLIENT_FEE,
                            FeeSizeType.PERCENTAGE,
                            BigDecimal.valueOf(0.1),
                            null,
                            null,
                            null,
                            "FeeTargetClient",
                            listOf("BTC"),
                            null
                        )
                    )
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent

        assertEquals(1, event.balanceUpdates?.size)
        assertEquals("Client1", event.balanceUpdates!!.single().walletId)
        assertEquals("EUR", event.balanceUpdates!!.single().assetId)
        assertEquals("100", event.balanceUpdates!!.single().oldReserved)
        assertEquals("50", event.balanceUpdates!!.single().newReserved)

        assertEquals(2, event.orders.size)

        val incomingLimitOrder = event.orders.single { it.walletId == "Client2" }
        assertEquals(OutgoingOrderStatus.REJECTED, incomingLimitOrder.status)
        assertEquals(OrderRejectReason.INVALID_FEE, incomingLimitOrder.rejectReason)
        assertEquals(0, incomingLimitOrder.trades?.size)

        val cancelledLimitOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(OutgoingOrderStatus.CANCELLED, cancelledLimitOrder.status)
        assertEquals(0, cancelledLimitOrder.trades?.size)

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)
        assertEquals(BigDecimal.valueOf(1.2), genericLimitOrderService.getOrderBook("", "EURUSD").getAskPrice())

        assertBalance("Client1", "EUR", 100.0, 50.0)
    }

    @Test
    fun testMatchWithExpiredOrder() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 20.0,
                    price = 1.1
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "1", clientId = "Client1", assetId = "EURUSD", volume = 20.0, price = 1.2,
                    timeInForce = OrderTimeInForce.GTD,
                    expiryTime = Date(Date().time + 500)
                )
            )
        )

        Thread.sleep(600)

        assertEquals(1, expiryOrdersQueue.getExpiredOrdersExternalIds(Date()).size)

        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "EURUSD",
                    volume = -15.0,
                    price = 1.1
                )
            )
        )

        assertEquals(0, expiryOrdersQueue.getExpiredOrdersExternalIds(Date()).size)
        assertOrderBookSize("EURUSD", true, 1)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent

        assertEquals(3, event.orders.size)

        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)

        assertBalance("Client1", "USD", reserved = 5.5)
        assertBalance("Client2", "USD", 1016.5)
    }

    @Test
    fun testExpiredOrder() {
        val order = buildLimitOrder(
            clientId = "Client1", assetId = "EURUSD", volume = 1.0, price = 1.0,
            timeInForce = OrderTimeInForce.GTD,
            expiryTime = Date()
        )
        Thread.sleep(10)
        singleLimitOrderService.processMessage(messageBuilder.buildLimitOrderWrapper(order))

        assertEquals(0, expiryOrdersQueue.getExpiredOrdersExternalIds(Date()).size)
        assertOrderBookSize("EURUSD", true, 0)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single().status)
    }

    @Test
    fun testImmediateOrCancelOrderWithTrades() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.2
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.3
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.4
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 30.0,
                    price = 1.3,
                    timeInForce = OrderTimeInForce.IOC
                )
            )
        )

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertBalance("Client1", "EUR", 1020.0, 0.0)
        assertBalance("Client1", "USD", 975.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(4, event.balanceUpdates?.size)

        val eventOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(2, eventOrder.trades?.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, eventOrder.status)
    }

    @Test
    fun testImmediateOrCancelOrderWithoutTrades() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.4
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 30.0,
                    price = 1.3,
                    timeInForce = OrderTimeInForce.IOC
                )
            )
        )

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(0, event.balanceUpdates?.size)

        val eventOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(0, eventOrder.trades?.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, eventOrder.status)
    }

    @Test
    fun testImmediateOrCancelOrderFullyMatched() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.2
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.3
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 15.0,
                    price = 1.4,
                    timeInForce = OrderTimeInForce.IOC
                )
            )
        )

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertBalance("Client1", "EUR", 1015.0, 0.0)
        assertBalance("Client1", "USD", 981.5, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(4, event.balanceUpdates?.size)

        val eventOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals(2, eventOrder.trades?.size)
        assertEquals(OutgoingOrderStatus.MATCHED, eventOrder.status)
    }

    @Test
    fun testFillOrKillOrderWithTrades() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.2
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.3
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.4
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 30.0,
                    price = 1.3,
                    timeInForce = OrderTimeInForce.FOK
                )
            )
        )

        assertOrderBookSize("EURUSD", false, 3)
        assertOrderBookSize("EURUSD", true, 0)

        assertBalance("Client1", "EUR", 1000.0, 0.0)
        assertBalance("Client1", "USD", 1000.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(0, event.balanceUpdates?.size)

        val eventOrder = event.orders.single()
        assertEquals("Client1", eventOrder.walletId)
        assertEquals(0, eventOrder.trades?.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, eventOrder.status)
    }

    @Test
    fun testFillOrKillOrderWithoutTrades() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.4
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 30.0,
                    price = 1.3,
                    timeInForce = OrderTimeInForce.FOK
                )
            )
        )

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(0, event.balanceUpdates?.size)

        val eventOrder = event.orders.single()
        assertEquals("Client1", eventOrder.walletId)
        assertEquals(0, eventOrder.trades?.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, eventOrder.status)
    }

    @Test
    fun testFillOrKillOrderFullyMatched() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.2
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "EURUSD",
                volume = -10.0,
                price = 1.3
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 15.0,
                    price = 1.4,
                    timeInForce = OrderTimeInForce.FOK
                )
            )
        )

        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 0)

        assertBalance("Client1", "EUR", 1015.0, 0.0)
        assertBalance("Client1", "USD", 981.5, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        assertEquals(4, event.balanceUpdates?.size)

        val eventOrder = event.orders.single { it.walletId == "Client1" }
        assertEquals("Client1", eventOrder.walletId)
        assertEquals(2, eventOrder.trades?.size)
        assertEquals(OutgoingOrderStatus.MATCHED, eventOrder.status)
    }
}