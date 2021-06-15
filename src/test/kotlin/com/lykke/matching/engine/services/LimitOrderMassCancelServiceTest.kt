package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderWrapper
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
import kotlin.test.assertEquals
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus


@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (LimitOrderMassCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class LimitOrderMassCancelServiceTest : AbstractTest() {
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

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {

        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 5))
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 2))

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("Client1", "USD", 100.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "EUR", 10.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "USD", 10.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 1.0)

        initServices()
    }

    private fun setOrders() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "1",
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    volume = -0.5,
                    price = 9000.0
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "2",
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    volume = -0.1,
                    price = 9000.0
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "3",
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    volume = 0.01,
                    price = 7000.0
                )
            )
        )

        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "4",
                    clientId = "Client1",
                    assetId = "EURUSD",
                    volume = 10.0,
                    price = 1.1
                )
            )
        )
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    uid = "5",
                    clientId = "Client1",
                    assetId = "BTCUSD",
                    type = LimitOrderType.STOP_LIMIT,
                    volume = 0.1,
                    lowerLimitPrice = 101.0,
                    lowerPrice = 100.0
                )
            )
        )

        multiLimitOrderService.processMessage(
            buildMultiLimitOrderWrapper(
                "EURUSD", "TrustedClient", listOf(
                    IncomingLimitOrder(-5.0, 1.3, "m1"),
                    IncomingLimitOrder(5.0, 1.1, "m2")
                )
            )
        )

        multiLimitOrderService.processMessage(
            buildMultiLimitOrderWrapper(
                "BTCUSD", "TrustedClient", listOf(
                    IncomingLimitOrder(-1.0, 8500.0)
                )
            )
        )

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 2)
        assertStopOrderBookSize("BTCUSD", true, 1)
        assertStopOrderBookSize("BTCUSD", false, 0)
        clearMessageQueues()
    }

    @Test
    fun testCancelOrdersOneSide() {
        setOrders()

        limitOrderMassCancelService.processMessage(
            messageBuilder.buildLimitOrderMassCancelWrapper(
                "Client1",
                "BTCUSD",
                false
            )
        )

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 2)
        assertStopOrderBookSize("BTCUSD", true, 1)
        assertStopOrderBookSize("BTCUSD", false, 0)

        assertBalance("Client1", "BTC", 1.0, 0.0)
        assertBalance("Client1", "USD", 100.0, 91.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(MessageType.LIMIT_ORDER_MASS_CANCEL.name, event.header.eventType)
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "2" }.status)
        assertEquals(1, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0.6", "0", event.balanceUpdates!!)
    }

    @Test
    fun cancelAllClientOrders() {
        setOrders()

        limitOrderMassCancelService.processMessage(messageBuilder.buildLimitOrderMassCancelWrapper("Client1"))

        assertOrderBookSize("BTCUSD", false, 1)
        assertOrderBookSize("BTCUSD", true, 0)
        assertOrderBookSize("EURUSD", false, 1)
        assertOrderBookSize("EURUSD", true, 1)
        assertStopOrderBookSize("BTCUSD", true, 0)
        assertStopOrderBookSize("BTCUSD", false, 0)

        assertBalance("Client1", "BTC", 1.0, 0.0)
        assertBalance("Client1", "USD", 100.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(MessageType.LIMIT_ORDER_MASS_CANCEL.name, event.header.eventType)
        assertEquals(5, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "1" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "2" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "3" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "4" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "5" }.status)
        assertEquals(2, event.balanceUpdates?.size)
        assertEventBalanceUpdate("Client1", "BTC", "1", "1", "0.6", "0", event.balanceUpdates!!)
        assertEventBalanceUpdate("Client1", "USD", "100", "100", "91", "0", event.balanceUpdates!!)
    }

    @Test
    fun testCancelTrustedClientOrders() {
        setOrders()

        limitOrderMassCancelService.processMessage(
            messageBuilder.buildLimitOrderMassCancelWrapper(
                "TrustedClient",
                "EURUSD"
            )
        )

        assertOrderBookSize("BTCUSD", false, 3)
        assertOrderBookSize("BTCUSD", true, 1)
        assertOrderBookSize("EURUSD", false, 0)
        assertOrderBookSize("EURUSD", true, 1)

        assertEquals(0, clientsEventsQueue.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        val event = trustedClientsEventsQueue.poll() as ExecutionEvent
        assertEquals(MessageType.LIMIT_ORDER_MASS_CANCEL.name, event.header.eventType)
        assertEquals(0, event.balanceUpdates?.size)
        assertEquals(2, event.orders.size)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "m1" }.status)
        assertEquals(OutgoingOrderStatus.CANCELLED, event.orders.single { it.externalId == "m2" }.status)
    }
}