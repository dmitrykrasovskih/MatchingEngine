package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderRejectReason
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMarketOrderWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
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
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (InvalidBalanceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class InvalidBalanceTest : AbstractTest() {

    @Autowired
    private
    lateinit var testSettingDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "ETH", 8))

            return testDictionariesDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "ETHUSD", "ETH", "USD", 5))
        initServices()
    }

    @Test
    fun testLimitOrderLeadsToInvalidBalance() {

        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 0.02)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client1", assetId = "USD", reservedBalance = 0.0)
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "ETH", balance = 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client2", assetId = "ETH", reservedBalance = 0.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "ETHUSD",
                volume = -0.000005,
                price = 1000.0
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "ETHUSD",
                volume = -0.000005,
                price = 1000.0
            )
        )

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "ETHUSD",
                    price = 1000.0,
                    volume = 0.00002
                )
            )
        )

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.orders.size)
        assertEquals(OutgoingOrderStatus.REJECTED, event.orders.single().status)
        assertEquals(OrderRejectReason.NOT_ENOUGH_FUNDS, event.orders.single().rejectReason)

        assertEquals(0, testOrderBookListener.getCount())

        assertEquals(0, genericLimitOrderService.getOrderBook("", "ETHUSD").getOrderBook(true).size)
        assertEquals(2, genericLimitOrderService.getOrderBook("", "ETHUSD").getOrderBook(false).size)
        genericLimitOrderService.getOrderBook("", "ETHUSD").getOrderBook(false).forEach {
            assertEquals("Client2", it.clientId)
            assertEquals(BigDecimal.valueOf(-0.000005), it.remainingVolume)
            assertEquals(OrderStatus.InOrderBook.name, it.status)
        }

        assertBalance("Client1", "USD", 0.02, 0.0)
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getBalance("Client1", "ETH"))

        assertBalance("Client2", "ETH", 1000.0, 0.0)
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedForOrdersBalance("", "", "Client2", "USD"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))
    }

    @Test
    fun testMarketOrderWithPreviousInvalidBalance() {
        testBalanceHolderWrapper.updateBalance(clientId = "Client1", assetId = "USD", balance = 0.02)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client1", assetId = "USD", reservedBalance = 0.0)

        // invalid opposite wallet
        testBalanceHolderWrapper.updateBalance(clientId = "Client2", assetId = "ETH", balance = 1.0)
        testBalanceHolderWrapper.updateReservedBalance(clientId = "Client2", assetId = "ETH", reservedBalance = 1.1)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "ETHUSD",
                volume = -0.000005,
                price = 1000.0
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "ETHUSD",
                volume = -0.000005,
                price = 1000.0
            )
        )

        initServices()

        marketOrderService.processMessage(
            buildMarketOrderWrapper(
                buildMarketOrder(
                    clientId = "Client1",
                    assetId = "ETHUSD",
                    volume = 0.00001
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(3, event.orders.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.MATCHED, it.status)
        }

        assertEquals(0, genericLimitOrderService.getOrderBook("", "ETHUSD").getOrderBook(true).size)
        assertEquals(0, genericLimitOrderService.getOrderBook("", "ETHUSD").getOrderBook(false).size)

        assertBalance("Client1", "USD", 0.0, 0.0)
        assertEquals(BigDecimal.valueOf(0.00001), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.00001), testWalletDatabaseAccessor.getBalance("Client1", "ETH"))

        assertBalance("Client2", "ETH", 0.99999, 1.09999)
        assertEquals(BigDecimal.valueOf(0.02), balancesHolder.getBalance("Client2", "USD"))
        assertEquals(BigDecimal.valueOf(0.02), testWalletDatabaseAccessor.getBalance("Client2", "USD"))
    }

    @Test
    fun testNegativeBalanceDueToTransferWithOverdraftLimit() {
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 3.0)
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 3.0)

        initServices()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "ETHUSD",
                    price = 1.0,
                    volume = 3.0
                )
            )
        )

        assertBalance("Client1", "USD", 3.0, 3.0)

        cashTransferOperationsService.processMessage(
            messageBuilder.buildTransferWrapper(
                "Client1",
                "Client2",
                "USD",
                4.0,
                4.0
            )
        )

        assertBalance("Client1", "USD", -1.0, 3.0)

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client1",
                    assetId = "ETHUSD",
                    price = 1.1,
                    volume = -0.5
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "ETHUSD",
                    price = 1.1,
                    volume = 0.5
                )
            )
        )

        assertBalance("Client1", "USD", -0.45, 3.0)

        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client2",
                    assetId = "ETHUSD",
                    price = 1.0,
                    volume = -0.5
                )
            )
        )

        assertBalance("Client1", "USD", -0.45, 0.0)

        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.first { it.walletId == "Client1" }.status)

    }

    @Test
    fun testMultiLimitOrderWithNotEnoughReservedFunds() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.25)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 275.0)

        initServices()

        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.TRUSTED_CLIENTS,
            "Client1",
            "Client1",
            true
        )

        multiLimitOrderService.processMessage(
            buildMultiLimitOrderWrapper(
                "ETHUSD", "Client1", listOf(
                    IncomingLimitOrder(-0.1, 1000.0, "1"),
                    IncomingLimitOrder(-0.05, 1010.0, "2"),
                    IncomingLimitOrder(-0.1, 1100.0, "3")
                )
            )
        )
        testBalanceHolderWrapper.updateReservedBalance("Client1", "ETH", reservedBalance = 0.09)
        testSettingDatabaseAccessor.clear()
        applicationSettingsCache.update()
        applicationSettingsHolder.update()

        clearMessageQueues()
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "4",
                    clientId = "Client2",
                    assetId = "ETHUSD",
                    volume = 0.25,
                    price = 1100.0
                )
            )
        )

        assertEquals(BigDecimal.valueOf(0.2), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.04), balancesHolder.getReservedForOrdersBalance("", "", "Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(0.05), balancesHolder.getBalance("Client2", "ETH"))
        assertEquals(BigDecimal.valueOf(224.5), balancesHolder.getBalance("Client2", "USD"))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(4, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.MATCHED, event.orders.single { it.externalId == "2" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "3" }.status)
        assertEquals(OutgoingOrderStatus.PARTIALLY_MATCHED, event.orders.single { it.externalId == "4" }.status)
    }

    @Test
    fun `Test multi limit order with enough reserved but not enough main balance`() {
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.1)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "ETH", reservedBalance = 0.05)
        testBalanceHolderWrapper.updateBalance("Client2", "USD", 275.0)

        initServices()
        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.TRUSTED_CLIENTS,
            "Client1",
            "Client1",
            true
        )

        multiLimitOrderService.processMessage(
            buildMultiLimitOrderWrapper(
                "ETHUSD", "Client1",
                listOf(IncomingLimitOrder(-0.05, 1010.0, "1"))
            )
        )
        testBalanceHolderWrapper.updateBalance("Client1", "ETH", 0.04)
        testSettingDatabaseAccessor.clear()
        applicationSettingsCache.update()
        applicationSettingsHolder.update()

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "2",
                    clientId = "Client2",
                    assetId = "ETHUSD",
                    volume = 0.25,
                    price = 1100.0
                )
            )
        )

        assertEquals(BigDecimal.valueOf(0.04), balancesHolder.getBalance("Client1", "ETH"))
        assertEquals(BigDecimal.valueOf(275.0), balancesHolder.getReservedForOrdersBalance("", "", "Client2", "USD"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getReservedForOrdersBalance("", "", "Client1", "ETH"))
        assertEquals(BigDecimal.ZERO, balancesHolder.getBalance("Client2", "ETH"))
        assertEquals(BigDecimal.valueOf(275.0), balancesHolder.getBalance("Client2", "USD"))

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.PLACED, event.orders.single { it.externalId == "2" }.status)
    }
}