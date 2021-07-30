package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
import com.lykke.matching.engine.utils.createAssetPair
import com.lykke.matching.engine.utils.getSetting
import org.junit.After
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
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (PersistenceErrorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class PersistenceErrorTest : AbstractTest() {

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))
            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        fun testConfig(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(
                AvailableSettingGroup.TRUSTED_CLIENTS,
                getSetting("TrustedClient")
            )
            return testSettingsDatabaseAccessor
        }
    }

    private val clientIds = listOf("Client1", "Client2", "Client3", "TrustedClient")

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        clientIds.forEach { clientId ->
            testBalanceHolderWrapper.updateBalance(clientId, "EUR", 1000.0)
            testBalanceHolderWrapper.updateBalance(clientId, "USD", 2000.0)
            testBalanceHolderWrapper.updateBalance(clientId, "BTC", 1.0)
        }

        testDictionariesDatabaseAccessor.addAssetPair(
            createAssetPair(
                "",
                "EURUSD",
                "EUR",
                "USD",
                5,
                BigDecimal.valueOf(0.05)
            )
        )
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 5))

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1", assetId = "EURUSD", volume = 1.0, price = 2.0, uid = "order1"
                )
            )
        )

        multiLimitOrderService.processMessage(
            buildMultiLimitOrderWrapper(
                "EURUSD", "TrustedClient",
                listOf(
                    IncomingLimitOrder(-1.0, 3.0, "order2"),
                    IncomingLimitOrder(-2.0, 3.1, "order3"),
                    IncomingLimitOrder(-3.0, 3.2, "order4"),
                    IncomingLimitOrder(-4.0, 3.3, "order5")
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client3", assetId = "EURUSD", volume = -5.0, price = 4.0, uid = "order6"
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1", assetId = "EURUSD", volume = -1.0,
                    type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 2.8, upperPrice = 2.5, uid = "stopOrder1"
                )
            )
        )

        clearMessageQueues()

        persistenceManager.persistenceErrorMode = true
    }

    @After
    override fun tearDown() {
        super.tearDown()
        persistenceManager.persistenceErrorMode = false
    }

    @Test
    fun testCashInOutOperation() {
        cashInOutOperationService.processMessage(messageBuilder.buildCashInOutWrapper("Client1", "EUR", 5.0))
        assertData()
        assertEquals(0, clientsEventsQueue.size)

        cashInOutOperationService.processMessage(messageBuilder.buildCashInOutWrapper("Client1", "EUR", -4.0))
        assertData()
        assertEquals(0, clientsEventsQueue.size)
    }

    @Test
    fun testTransferOperation() {
        cashTransferOperationsService.processMessage(
            messageBuilder.buildTransferWrapper(
                "Client1", "Client2",
                "BTC", 0.1, 0.0
            )
        )
        assertData()
        assertEquals(0, clientsEventsQueue.size)
    }

//    @Test
//    fun testLimitOrderCancel() {
//        // Limit Order
//        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("order1"))
//        assertData()
//        assertEquals(0, clientsEventsQueue.size)
//        assertEquals(0, trustedClientsEventsQueue.size)
//
//        // Stop Limit Order
//        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("stopOrder1"))
//        assertData()
//        assertEquals(0, clientsEventsQueue.size)
//        assertEquals(0, trustedClientsEventsQueue.size)
//    }
//
//    @Test
//    fun testLimitOrderMassCancel() {
//        limitOrderMassCancelService.processMessage(messageBuilder.buildLimitOrderMassCancelWrapper("Client1"))
//        assertData()
//        assertEquals(0, clientsEventsQueue.size)
//        assertEquals(0, trustedClientsEventsQueue.size)
//    }
//
//    @Test
//    fun testMultiLimitOrderCancel() {
//        val messageWrapper = buildMultiLimitOrderCancelWrapper("TrustedClient", "EURUSD", false)
//        multiLimitOrderCancelService.processMessage(messageWrapper)
//        assertData()
//        assertEquals(0, clientsEventsQueue.size)
//        assertEquals(0, trustedClientsEventsQueue.size)
//    }
//
//    @Test
//    fun testClientMultiLimitOrderCancel() {
//        val messageWrapper = buildMultiLimitOrderCancelWrapper("Client1", "EURUSD", true)
//        multiLimitOrderCancelService.processMessage(messageWrapper)
//        assertData()
//        assertEquals(0, clientsEventsQueue.size)
//        assertEquals(0, trustedClientsEventsQueue.size)
//    }
//
//    @Test
//    fun testMarketOrder() {
//        marketOrderService.processMessage(
//            buildMarketOrderWrapper(
//                buildMarketOrder(
//                    clientId = "Client2", assetId = "EURUSD", volume = -0.9
//                )
//            )
//        )
//        assertMarketOrderResult()
//
//        // Uncompleted limit order has min volume
//        marketOrderService.processMessage(
//            buildMarketOrderWrapper(
//                buildMarketOrder(
//                    clientId = "Client2", assetId = "EURUSD", volume = -0.96
//                )
//            )
//        )
//        assertMarketOrderResult()
//    }
//
//    @Test
//    fun testLimitOrder() {
//        // Add order
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client2", assetId = "BTCUSD", volume = -0.5, price = 1000.0
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client1", assetId = "EURUSD", volume = -1.0, price = 2.2
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client1", assetId = "EURUSD", volume = 1.0, price = 2.1
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client1", assetId = "EURUSD", volume = 1.0, price = 2.1
//                ), cancel = true
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add and match
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client3", assetId = "EURUSD", volume = -4.0, price = 2.0
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client3", assetId = "EURUSD", volume = -4.0, price = 2.0
//                ), cancel = true
//            )
//        )
//        assertLimitOrderResult()
//
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client3", assetId = "EURUSD", volume = -0.9, price = 2.0
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add and match, uncompleted limit order has min volume
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client3", assetId = "EURUSD", volume = -0.96, price = 2.0
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add and match with several orders
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client3", assetId = "EURUSD", volume = 5.96, price = 3.2
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add and match with pending stop order
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client3", assetId = "EURUSD", volume = 1.0, price = 2.8
//                )
//            )
//        )
//        assertLimitOrderResult()
//    }
//
//    @Test
//    fun testStopLimitOrder() {
//        // Add stop order
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client1", assetId = "EURUSD", volume = -1.0,
//                    type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 2.7, upperPrice = 2.4
//                )
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add stop order and cancel previous
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client1", assetId = "EURUSD", volume = -1.0,
//                    type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 2.7, upperPrice = 2.4
//                ), cancel = true
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add invalid stop order and cancel previous
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client1", assetId = "EURUSD", volume = -100000000.0,
//                    type = LimitOrderType.STOP_LIMIT, upperLimitPrice = 2.7, upperPrice = 2.4
//                ), cancel = true
//            )
//        )
//        assertLimitOrderResult()
//
//        // Add stop order which will be processed immediately
//        singleLimitOrderService.processMessage(
//            messageBuilder.buildLimitOrderWrapper(
//                buildLimitOrder(
//                    clientId = "Client2", assetId = "EURUSD", volume = -1.0,
//                    type = LimitOrderType.STOP_LIMIT, lowerLimitPrice = 2.0, lowerPrice = 1.8
//                )
//            )
//        )
//        assertLimitOrderResult()
//    }
//
//    @Test
//    fun testTrustedClientMultiLimitOrder() {
//        multiLimitOrderService.processMessage(
//            buildMultiLimitOrderWrapper(
//                "EURUSD", "TrustedClient",
//                listOf(
//                    IncomingLimitOrder(-1.0, 3.1),
//                    IncomingLimitOrder(-2.0, 3.19),
//                    IncomingLimitOrder(-3.0, 3.29)
//                ), cancel = true
//            )
//        )
//
//        assertData()
//        assertEquals(0, clientsEventsQueue.size)
//        assertEquals(0, trustedClientsEventsQueue.size)
//    }
//
//    @Test
//    fun testClientMultiLimitOrder() {
//        multiLimitOrderService.processMessage(
//            buildMultiLimitOrderWrapper(
//                "EURUSD", "Client3",
//                listOf(
//                    IncomingLimitOrder(-1.0, 3.1),
//                    IncomingLimitOrder(-2.0, 3.19),
//                    IncomingLimitOrder(-3.0, 3.29)
//                ), cancel = true
//            )
//        )
//
//        assertMultiLimitOrderResult()
//
//        multiLimitOrderService.processMessage(
//            buildMultiLimitOrderWrapper(
//                "EURUSD", "Client3",
//                listOf(
//                    IncomingLimitOrder(-0.04, 5.1),
//                    IncomingLimitOrder(-2.0, 5.2),
//                    IncomingLimitOrder(-3.0, 5.3),
//                    IncomingLimitOrder(0.96, 4.0),
//                    IncomingLimitOrder(2.5, 3.3),
//                    IncomingLimitOrder(0.9, 3.1),
//                    IncomingLimitOrder(0.1, 2.9)
//                ), cancel = true
//            )
//        )
//
//        assertMultiLimitOrderResult()
//    }

    private fun assertMarketOrderResult() {
        assertData()
        assertEquals(0, clientsEventsQueue.size)
        assertEquals(0, trustedClientsEventsQueue.size)
    }

    private fun assertLimitOrderResult() {
        assertMultiLimitOrderResult()
    }

    private fun assertMultiLimitOrderResult() {
        assertData()
        assertEquals(0, clientsEventsQueue.size)
        assertEquals(0, trustedClientsEventsQueue.size)
    }

    private fun assertData() {
        assertBalances()
        assertOrderBooks()
    }

    private fun assertBalances() {
        assertBalance("Client1", "EUR", 1000.0, 1.0)
        assertBalance("Client1", "USD", 2000.0, 2.0)
        assertBalance("Client1", "BTC", 1.0, 0.0)

        assertBalance("Client2", "EUR", 1000.0, 0.0)
        assertBalance("Client2", "USD", 2000.0, 0.0)
        assertBalance("Client2", "BTC", 1.0, 0.0)

        assertBalance("Client3", "EUR", 1000.0, 5.0)
        assertBalance("Client3", "USD", 2000.0, 0.0)
        assertBalance("Client3", "BTC", 1.0, 0.0)

        assertBalance("TrustedClient", "EUR", 1000.0, 0.0)
        assertBalance("TrustedClient", "USD", 2000.0, 0.0)
        assertBalance("TrustedClient", "BTC", 1.0, 0.0)
    }

    private fun assertOrderBooks() {
        assertOrderBookSize("EURUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 5)

        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("BTCUSD", false, 0)

        assertStopOrderBookSize("EURUSD", true, 0)
        assertStopOrderBookSize("EURUSD", false, 1)

        assertStopOrderBookSize("BTCUSD", true, 0)
        assertStopOrderBookSize("BTCUSD", false, 0)

        assertOrder(genericLimitOrderService.getOrder("order1"), OrderStatus.InOrderBook)
        assertOrder(genericLimitOrderService.getOrder("order2"), OrderStatus.InOrderBook)
        assertOrder(genericLimitOrderService.getOrder("order3"), OrderStatus.InOrderBook)
        assertOrder(genericLimitOrderService.getOrder("order4"), OrderStatus.InOrderBook)
        assertOrder(genericLimitOrderService.getOrder("order5"), OrderStatus.InOrderBook)
        assertOrder(genericLimitOrderService.getOrder("order6"), OrderStatus.InOrderBook)
        assertOrder(genericStopLimitOrderService.getOrder("stopOrder1"), OrderStatus.Pending)
    }

    private fun assertOrder(order: LimitOrder?, status: OrderStatus) {
        assertNotNull(order)
        assertEquals(status.name, order.status)
    }


}