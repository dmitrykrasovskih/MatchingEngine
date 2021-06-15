package com.lykke.matching.engine.services

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.IncomingLimitOrder
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.outgoing.messages.v2.enums.OrderStatus
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

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (NegativePriceTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class NegativePriceTest : AbstractTest() {

    @Autowired
    private lateinit var testSettingsDatabaseAccessor: TestSettingsDatabaseAccessor

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

    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private lateinit var testConfigDatabaseAccessor: TestSettingsDatabaseAccessor

    @Autowired
    private lateinit var messageBuilder: MessageBuilder

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance("Client", "USD", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client", "USD", 0.0)

        testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 5))

        initServices()
    }

    @Test
    fun testLimitOrder() {
        singleLimitOrderService.processMessage(
            messageBuilder.buildLimitOrderWrapper(
                buildLimitOrder(
                    clientId = "Client",
                    assetId = "EURUSD",
                    price = -1.0,
                    volume = 1.0
                )
            )
        )

        assertEquals(1, clientsEventsQueue.size)
        val result = clientsEventsQueue.poll() as ExecutionEvent

        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.REJECTED, result.orders.first().status)
    }

    @Test
    fun testTrustedClientMultiLimitOrder() {
        testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.TRUSTED_CLIENTS, getSetting("Client"))

        initServices()

        multiLimitOrderService.processMessage(
            buildMultiLimitOrderWrapper(
                "EURUSD",
                "Client",
                listOf(
                    IncomingLimitOrder(1.0, 1.0, "order1"),
                    IncomingLimitOrder(1.0, -1.0, "order2")
                )
            )
        )

        assertEquals(1, trustedClientsEventsQueue.size)
        val result = trustedClientsEventsQueue.poll() as ExecutionEvent

        assertEquals(1, result.orders.size)
        assertEquals(OrderStatus.PLACED, result.orders.first { it.externalId == "order1" }.status)

        assertEquals(1, testOrderDatabaseAccessor.getOrders("EURUSD", true).size)
    }
}