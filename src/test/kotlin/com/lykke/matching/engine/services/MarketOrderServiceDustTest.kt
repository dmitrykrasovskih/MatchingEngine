package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
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
import kotlin.test.assertFalse
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MarketOrderServiceDustTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderServiceDustTest : AbstractTest() {

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()

            testDictionariesDatabaseAccessor.addAsset(Asset("", "LKK", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "SLR", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "GBP", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "CHF", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "JPY", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC1", 8))

            return testDictionariesDatabaseAccessor
        }
    }

    @Before
    fun setUp() {

        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURJPY", "EUR", "JPY", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 8))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCLKK", "BTC", "LKK", 6))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTC1USD", "BTC1", "USD", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTC1LKK", "BTC1", "LKK", 6))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCCHF", "BTC", "CHF", 3))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "SLRBTC", "SLR", "BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "SLRBTC1", "SLR", "BTC1", 8))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "LKKEUR", "LKK", "EUR", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "LKKGBP", "LKK", "GBP", 5))
        initServices()
    }

    @Test
    fun testDustMatchOneToOne() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTCUSD",
                price = 1000.0,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 0.020009)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    volume = -0.02
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("1000", eventMarketOrder.price)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("-0.02", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("BTC", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("20", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("USD", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.02), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(1480.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.000009), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
        assertEquals(BigDecimal.valueOf(20.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testDustIncorrectBalanceAndDust1() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTC1USD",
                price = 610.96,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.14441494999999982)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = 88.23,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("610.96", eventMarketOrder.price)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("-0.14441208", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("BTC1", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("88.23", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("USD", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.14441208), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(1500 - 88.23), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00000287), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(88.23), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testDustIncorrectBalanceAndDust2() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTC1USD",
                price = 598.916,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.033407)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = 20.0,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("598.916", eventMarketOrder.price)
        assertEquals("20", eventMarketOrder.volume)
        assertFalse(eventMarketOrder.straight!!)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("-0.03339367", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("BTC1", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("20", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("USD", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.03339367), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(1500 - 20.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00001333), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(20.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testDustIncorrectBalanceAndDust3() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                assetId = "BTC1USD",
                price = 593.644,
                volume = 1000.0,
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 1500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.00092519)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = 0.54,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("593.644", eventMarketOrder.price)
        assertEquals("0.54", eventMarketOrder.volume)
        assertFalse(eventMarketOrder.straight!!)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("-0.00090964", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("BTC1", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("0.54", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("USD", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.00090964), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(1500 - 0.54), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(0.00001555), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(0.54), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testDustNotStraight() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = 500.0,
                assetId = "BTCUSD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC", 0.02001)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTCUSD",
                    volume = 20.0,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("1000", eventMarketOrder.price)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("-0.02", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("BTC", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("20", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("USD", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)

        assertEquals(BigDecimal.valueOf(0.02), testWalletDatabaseAccessor.getBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(480.0), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(20.0), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(0.00001), testWalletDatabaseAccessor.getBalance("Client4", "BTC"))
    }

    @Test
    fun testBuyDustStraight() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = -500.0,
                assetId = "BTC1USD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "BTC1", 0.02001)
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = 0.0000272,
                    straight = true
                )
            )
        )
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals(4, event.balanceUpdates?.size)
    }

    @Test
    fun test_20170309_01() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 0.0000782,
                volume = -4000.0,
                assetId = "SLRBTC1",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "SLR", 238619.65864945)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.01)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "SLRBTC1",
                    volume = 127.87,
                    straight = true
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("127.87", eventMarketOrder.volume)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("127.87", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("SLR", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("-0.00999944", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("BTC1", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)
    }

    @Test
    fun test_20170309_02() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 0.0000782,
                volume = -4000.0,
                assetId = "SLRBTC1",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "SLR", 238619.65864945)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.01)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "SLRBTC1",
                    volume = -0.01,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        assertEquals("Client4", eventMarketOrder.walletId)
        assertEquals("-0.01", eventMarketOrder.volume)
        assertEquals(1, eventMarketOrder.trades?.size)
        assertEquals("127.87", eventMarketOrder.trades!!.first().baseVolume)
        assertEquals("SLR", eventMarketOrder.trades!!.first().baseAssetId)
        assertEquals("-0.01", eventMarketOrder.trades!!.first().quotingVolume)
        assertEquals("BTC1", eventMarketOrder.trades!!.first().quotingAssetId)
        assertEquals("Client3", eventMarketOrder.trades!!.first().oppositeWalletId)
    }

    @Test
    fun testSellDustStraight() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = 500.0,
                assetId = "BTC1USD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.02001)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = -0.0000272,
                    straight = true
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
    }

    @Test
    fun testBuyDustNotStraight() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 19739.43939992,
                volume = 500.0,
                assetId = "BTC1LKK",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "LKK", 500.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 0.02001)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1LKK",
                    volume = 0.01,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
    }

    @Test
    fun testSellDustNotStraight() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 19739.43939992,
                volume = -500.0,
                assetId = "BTC1LKK",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "BTC1", 0.02001)
        testBalanceHolderWrapper.updateBalance("Client4", "LKK", 500.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1LKK",
                    volume = -0.01,
                    straight = false
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
    }

    @Test
    fun testDust1() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = -0.05,
                assetId = "BTC1USD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 5000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC1", 10.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = 0.04997355,
                    straight = true
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        val eventLimitOrder = event.orders.single { it.orderType == OrderType.LIMIT }
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, eventLimitOrder.status)

        assertEquals(BigDecimal.valueOf(0.04997355), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(4950.02), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(10 - 0.04997355), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(49.98), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
    }

    @Test
    fun testDust2() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = 0.05,
                assetId = "BTC1USD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 5000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 10.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = -0.04997355,
                    straight = true
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        val eventLimitOrder = event.orders.single { it.orderType == OrderType.LIMIT }
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, eventLimitOrder.status)

        assertEquals(BigDecimal.valueOf(0.04997355), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(4950.03), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(10 - 0.04997355), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(49.97), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }

    @Test
    fun testDust3() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = -0.05,
                assetId = "BTC1USD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client4", "USD", 5000.0)
        testBalanceHolderWrapper.updateBalance("Client3", "BTC1", 10.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = 0.0499727,
                    straight = true
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        val eventLimitOrder = event.orders.single { it.orderType == OrderType.LIMIT }
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, eventLimitOrder.status)

        assertEquals(BigDecimal.valueOf(0.0499727), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(4950.02), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
        assertEquals(BigDecimal.valueOf(9.9500273), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(49.98), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
    }

    @Test
    fun testDust4() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                price = 1000.0,
                volume = 0.05,
                assetId = "BTC1USD",
                clientId = "Client3"
            )
        )
        testBalanceHolderWrapper.updateBalance("Client3", "USD", 5000.0)
        testBalanceHolderWrapper.updateBalance("Client4", "BTC1", 10.0)
        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client4",
                    assetId = "BTC1USD",
                    volume = -0.0499727,
                    straight = true
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        val eventMarketOrder = event.orders.single { it.orderType == OrderType.MARKET }
        assertEquals(OutgoingOrderStatus.MATCHED, eventMarketOrder.status)
        val eventLimitOrder = event.orders.single { it.orderType == OrderType.LIMIT }
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, eventLimitOrder.status)

        assertEquals(BigDecimal.valueOf(0.0499727), testWalletDatabaseAccessor.getBalance("Client3", "BTC1"))
        assertEquals(BigDecimal.valueOf(4950.03), testWalletDatabaseAccessor.getBalance("Client3", "USD"))
        assertEquals(BigDecimal.valueOf(9.9500273), testWalletDatabaseAccessor.getBalance("Client4", "BTC1"))
        assertEquals(BigDecimal.valueOf(49.97), testWalletDatabaseAccessor.getBalance("Client4", "USD"))
    }
}