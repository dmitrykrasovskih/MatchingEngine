package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.v2.events.ExecutionEvent
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildMultiLimitOrderCancelWrapper
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
import kotlin.test.assertEquals
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus as OutgoingOrderStatus

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MultiLimitOrderCancelServiceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MultiLimitOrderCancelServiceTest : AbstractTest() {

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))

            return testDictionariesDatabaseAccessor
        }
    }

    @Before
    fun setUp() {
        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 8))

        testSettingsDatabaseAccessor.createOrUpdateSetting(
            AvailableSettingGroup.TRUSTED_CLIENTS,
            getSetting("TrustedClient")
        )

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient", "BTC", 1.0)

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                volume = -0.4,
                price = 10000.0,
                reservedVolume = 0.4
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                volume = -0.6,
                price = 11000.0,
                reservedVolume = 0.6
            )
        )

        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "TrustedClient",
                assetId = "BTCUSD",
                volume = -0.3,
                price = 10500.0
            )
        )
        val partiallyMatchedTrustedClientOrder =
            buildLimitOrder(clientId = "TrustedClient", assetId = "BTCUSD", volume = -0.7, price = 11500.0)
        partiallyMatchedTrustedClientOrder.remainingVolume = BigDecimal.valueOf(-0.6)
        testOrderBookWrapper.addLimitOrder(partiallyMatchedTrustedClientOrder)

        initServices()
    }

    @Test
    fun testCancelTrustedClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("TrustedClient", "BTCUSD", false)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("TrustedClient", "BTC", 1.0, 0.0)

        assertEquals(1, clientsEventsQueue.size)
        assertEquals(1, (clientsEventsQueue.first() as ExecutionEvent).orders.size)
        assertEquals(0, (clientsEventsQueue.first() as ExecutionEvent).balanceUpdates!!.size)
        assertEquals(1, trustedClientsEventsQueue.size)
        assertEquals(1, (trustedClientsEventsQueue.first() as ExecutionEvent).orders.size)
        assertEquals(0, (trustedClientsEventsQueue.first() as ExecutionEvent).balanceUpdates!!.size)
    }

    @Test
    fun testCancelClientOrders() {
        val messageWrapper = buildMultiLimitOrderCancelWrapper("Client1", "BTCUSD", false)
        multiLimitOrderCancelService.processMessage(messageWrapper)

        assertOrderBookSize("BTCUSD", false, 2)
        assertBalance("Client1", "BTC", 1.0, 0.0)

        assertEquals(0, trustedClientsEventsQueue.size)
        assertEquals(1, clientsEventsQueue.size)
        val event = clientsEventsQueue.poll() as ExecutionEvent
        assertEquals(1, event.balanceUpdates?.size)
        event.orders.forEach {
            assertEquals(OutgoingOrderStatus.CANCELLED, it.status)
        }
    }
}