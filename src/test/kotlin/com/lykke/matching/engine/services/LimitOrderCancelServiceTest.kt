package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.assertEquals
import com.lykke.matching.engine.utils.createAssetPair
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
import kotlin.test.assertFalse
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderCancelServiceTest : AbstractTest() {
    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))

            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "5", price = 100.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "3", price = 300.0, volume = -1.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "6", price = 200.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "7", price = 300.0))
        testOrderBookWrapper.addLimitOrder(buildLimitOrder(uid = "8", price = 400.0))

        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURCHF", "EUR", "CHF", 5))

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 1000.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 1.0)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 1000.0)
        initServices()
    }

    @Test
    fun testCancel() {
        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper("3"))

        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedForOrdersBalance("", "", "Client1", "EUR"))

        val previousOrders = genericLimitOrderService.searchOrders("Client1", "EURUSD", true)
        assertEquals(4, previousOrders.size)
        assertFalse(previousOrders.any { it.externalId == "3" })

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, executionEvent.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "EUR", "1000", "1000", "1", "0", executionEvent.balanceUpdates!!)
        assertEquals(1, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders.first().status)
    }

    @Test
    fun testMultiCancel() {
        testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 5))
        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 1.0)
        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "10",
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 9000.0,
                    volume = -0.5
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "11",
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 9100.0,
                    volume = -0.3
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "12",
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 9200.0,
                    volume = -0.2
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "13",
                    clientId = "Client2",
                    assetId = "BTCUSD",
                    price = 8000.0,
                    volume = 0.1
                )
            )
        )
        clearMessageQueues()

        limitOrderCancelService.processMessage(messageBuilder.buildLimitOrderCancelWrapper(listOf("10", "11", "13")))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)

        assertBalance("Client2", "BTC", 1.0, 0.2)
        assertBalance("Client2", "USD", 1000.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val executionEvent = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, executionEvent.balanceUpdates?.size)

        assertEventBalanceUpdate("Client2", "BTC", "1", "1", "1", "0.2", executionEvent.balanceUpdates!!)
        assertEventBalanceUpdate("Client2", "USD", "1000", "1000", "800", "0", executionEvent.balanceUpdates!!)

        assertEquals(3, executionEvent.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders[0].status)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders[1].status)
        assertEquals(OutgoingOrderStatus.CANCELLED, executionEvent.orders[2].status)
    }
}