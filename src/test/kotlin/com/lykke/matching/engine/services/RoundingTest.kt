package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus.*
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderType
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.createAssetPair
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (RoundingTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RoundingTest : AbstractTest() {

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()


            testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "JPY", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "CHF", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "LKK", 0))
            return testDictionariesDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURJPY", "EUR", "JPY", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCEUR", "BTC", "EUR", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCLKK", "BTC", "LKK", 2))
        initServices()
    }

    @Test
    fun testStraightBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "EURUSD",
                    volume = 1.0
                )
            )
        )

        val marketOrderReport = (clientsEventsQueue.poll() as ExecutionEvent).orders.first()
        assertEquals(PARTIALLY_MATCHED, marketOrderReport.status)
        assertEquals("1.11548", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("1.12", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().)
        assertEquals("-1", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("EUR", marketOrderReport.trades!!.first().baseAssetId)
        assertEquals("Client4", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(1.12), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1498.88), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testStraightSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "EURUSD",
                    volume = -1.0
                )
            )
        )

        val marketOrderReport =
            (clientsEventsQueue.poll() as ExecutionEvent).orders.first { it.orderType == OrderType.MARKET }
        assertEquals(MATCHED, marketOrderReport.status)
        assertEquals("1.11", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("-1", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("EUR", marketOrderReport.trades!!.first().baseAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("1.11", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
        assertEquals("Client3", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(998.89), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1.11), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightBuy() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = 1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "EURUSD",
                    volume = 1.0,
                    straight = false
                )
            )
        )

        val marketOrderReport = (clientsEventsQueue.poll() as ExecutionEvent).orders.first()
        assertEquals(PARTIALLY_MATCHED, marketOrderReport.status)
        assertEquals("1.11548", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("0.9", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("EUR", marketOrderReport.trades!!.first().baseAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("-1", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
        assertEquals("Client4", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.9), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(1499.1), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
    }

    @Test
    fun testNotStraightSell() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(price = 1.11548, volume = -1000.0, clientId = "Client3"))
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "EURUSD",
                    volume = -1.0,
                    straight = false
                )
            )
        )

        val marketOrderReport =
            (clientsEventsQueue.poll() as ExecutionEvent).orders.first { it.orderType == OrderType.MARKET }
        assertEquals(MATCHED, marketOrderReport.status)
        assertEquals("1.12359", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("-1", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("0.89", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("EUR", marketOrderReport.trades!!.first().baseAssetId)
        assertEquals("Client3", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(999.11), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.89), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightSellRoundingError() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCCHF",
                price = 909.727,
                volume = -1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "CHF", 1.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCCHF",
                    volume = -0.38,
                    straight = false
                )
            )
        )

        val marketOrderReport =
            (clientsEventsQueue.poll() as ExecutionEvent).orders.first { it.orderType == OrderType.MARKET }
        assertEquals(MATCHED, marketOrderReport.status)
        assertEquals("909.743", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("-0.38", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("CHF", marketOrderReport.trades!!.first().quotingAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("0.0004177", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("BTC", marketOrderReport.trades!!.first().baseAssetId)
        assertEquals("Client3", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.9995823), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.38), testWalletDatabaseAccessor.getBalance("Client3", "CHF"))
        assertEquals(BigDecimal.valueOf(0.0004177), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(0.62), testWalletDatabaseAccessor.getBalance("Client4", "CHF"))
    }

    @Test
    fun testStraightBuyBTC() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCUSD",
                price = 678.229,
                volume = -1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    volume = 1.0
                )
            )
        )

        val marketOrderReport =
            (clientsEventsQueue.poll() as ExecutionEvent).orders.first { it.orderType == OrderType.MARKET }
        assertEquals(MATCHED, marketOrderReport.status)
        assertEquals("678.23", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("-678.23", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("1", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("BTC", marketOrderReport.trades!!.first().baseAssetId)
        assertEquals("Client3", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(678.23), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(821.77), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testStraightSellBTC() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCUSD",
                price = 678.229,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    volume = -1.0
                )
            )
        )

        val marketOrderReport = (clientsEventsQueue.poll() as ExecutionEvent).orders.first()
        assertEquals(PARTIALLY_MATCHED, marketOrderReport.status)
        assertEquals("678.229", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("1", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("BTC", marketOrderReport.trades!!.first().baseAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("-678.22", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
        assertEquals("Client4", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(321.78), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(678.22), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightBuyBTC() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCUSD",
                price = 678.229,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    volume = 1.0,
                    straight = false
                )
            )
        )

        val marketOrderReport = (clientsEventsQueue.poll() as ExecutionEvent).orders.first()
        assertEquals(PARTIALLY_MATCHED, marketOrderReport.status)
        assertEquals("678.229", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("0.00147443", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("BTC", marketOrderReport.trades!!.first().baseAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("-1", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
        assertEquals("Client4", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(999.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00147443), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(1499.99852557), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
    }

    @Test
    fun testNotStraightSellBTC() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCUSD",
                price = 678.229,
                volume = -1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 1500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    volume = -1.0,
                    straight = false
                )
            )
        )

        val marketOrderReport = (clientsEventsQueue.poll() as ExecutionEvent).orders.first()
        assertEquals(PARTIALLY_MATCHED, marketOrderReport.status)
        assertEquals("678.229", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("1", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("USD", marketOrderReport.trades!!.first().quotingAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("-0.00147442", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("BTC", marketOrderReport.trades!!.first().baseAssetId)
        assertEquals("Client4", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(999.99852558), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00147442), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(1499.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testNotStraightSellBTCMultiLevel() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCLKK",
                price = 14925.09,
                volume = -1.34,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCLKK",
                price = 14950.18,
                volume = -1.34,
                clientId = "Client3"
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCLKK",
                price = 14975.27,
                volume = -1.34,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "LKK", 50800.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCLKK",
                    volume = -50800.0,
                    straight = false
                )
            )
        )

        val marketOrderReport =
            (clientsEventsQueue.poll() as ExecutionEvent).orders.first { it.orderType == OrderType.MARKET }
        assertEquals(MATCHED, marketOrderReport.status)
        assertEquals("14945.93", marketOrderReport.price!!)
        assertEquals(3, marketOrderReport.trades!!.size)

        assertEquals(BigDecimal.valueOf(50800.0), testWalletDatabaseAccessor.getBalance("Client3", "LKK"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client4", "LKK"))
    }

    @Test
    fun testNotStraightBuyEURJPY() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "EURJPY",
                price = 116.356,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "JPY", 1000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "EUR", 0.00999999999999999)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "EURJPY",
                    volume = 1.16,
                    straight = false
                )
            )
        )

        val marketOrderReport = (clientsEventsQueue.poll() as ExecutionEvent).orders.first()
        assertEquals(REJECTED, marketOrderReport.status)
    }

    @Test
    fun testStraightSellBTCEUR() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 597.169,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCEUR",
                    volume = -0.0001
                )
            )
        )

        val marketOrderReport =
            (clientsEventsQueue.poll() as ExecutionEvent).orders.first { it.orderType == OrderType.MARKET }
        assertEquals(MATCHED, marketOrderReport.status)
        assertEquals("500", marketOrderReport.price!!)
        assertEquals(1, marketOrderReport.trades!!.size)

        assertEquals("-0.0001", marketOrderReport.trades!!.first().baseVolume)
        assertEquals("BTC", marketOrderReport.trades!!.first().baseAssetId)
//        assertEquals("Client4", marketOrderReport.trades!!.first().marketClientId)
        assertEquals("0.05", marketOrderReport.trades!!.first().quotingVolume)
        assertEquals("EUR", marketOrderReport.trades!!.first().quotingAssetId)
        assertEquals("Client3", marketOrderReport.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.0001), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.95), testWalletDatabaseAccessor.getBalance("Client3", "EUR"))
        assertEquals(BigDecimal.valueOf(0.9999), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(0.05), testWalletDatabaseAccessor.getBalance("Client4", "EUR"))
    }

    @Test
    fun testLimitOrderRounding() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCEUR",
                price = 1121.509,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "EUR", 1.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 1.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCEUR",
                    volume = -0.00043722
                )
            )
        )

        val limitOrder = testOrderDatabaseAccessor.getOrders("BTCEUR", true).singleOrNull()
        assertNotNull(limitOrder)
        assertEquals(BigDecimal.valueOf(1000.0 - 0.00043722), limitOrder.remainingVolume)
    }
}