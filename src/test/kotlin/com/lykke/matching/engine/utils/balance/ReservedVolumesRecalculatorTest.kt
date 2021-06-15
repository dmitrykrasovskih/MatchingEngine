package com.lykke.matching.engine.utils.balance

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestReservedVolumesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.database.TestWalletDatabaseAccessor
import com.lykke.matching.engine.holders.BalancesDatabaseAccessorsHolder
import com.lykke.matching.engine.order.utils.TestOrderBookWrapper
import com.lykke.matching.engine.outgoing.messages.v2.events.CashInEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.CashOutEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.Event
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
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
import java.util.concurrent.BlockingQueue
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (ReservedVolumesRecalculatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReservedVolumesRecalculatorTest {

    @TestConfiguration
    class Config {

        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", "USD", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "EUR", 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))
            testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "EURUSD", "EUR", "USD", 5))
            testDictionariesDatabaseAccessor.addAssetPair(createAssetPair("", "BTCUSD", "BTC", "USD", 8))

            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        fun testConfig(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(
                AvailableSettingGroup.TRUSTED_CLIENTS,
                getSetting("trustedClient")
            )
            testSettingsDatabaseAccessor.createOrUpdateSetting(
                AvailableSettingGroup.TRUSTED_CLIENTS,
                getSetting("trustedClient2")
            )
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    protected lateinit var clientsEventsQueue: BlockingQueue<Event<*>>

    @Autowired
    lateinit var testOrderBookWrapper: TestOrderBookWrapper

    @Autowired
    private
    lateinit var balancesDatabaseAccessorsHolder: BalancesDatabaseAccessorsHolder

    @Autowired
    lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Autowired
    lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    lateinit var reservedVolumesRecalculator: ReservedVolumesRecalculator

    @Autowired
    lateinit var reservedVolumesDatabaseAccessor: TestReservedVolumesDatabaseAccessor

    @Before
    fun setUp() {
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "trustedClient",
                assetId = "BTCUSD",
                price = 10000.0,
                volume = -1.0,
                reservedVolume = 0.5
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client1",
                assetId = "BTCUSD",
                price = 10000.0,
                volume = -1.0,
                reservedVolume = 0.5
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                uid = "1",
                clientId = "Client1",
                assetId = "EURUSD",
                price = 10000.0,
                volume = -1.0,
                reservedVolume = 0.4
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                uid = "2",
                clientId = "Client1",
                assetId = "EURUSD",
                price = 10000.0,
                volume = -1.0,
                reservedVolume = 0.3
            )
        )
        testOrderBookWrapper.addLimitOrder(
            buildLimitOrder(
                clientId = "Client2",
                assetId = "BTCUSD",
                price = 10000.0,
                volume = -1.0,
                reservedVolume = 1.0
            )
        )

        testOrderBookWrapper.addStopLimitOrder(
            buildLimitOrder(
                uid = "3",
                clientId = "Client2",
                assetId = "BTCUSD",
                type = LimitOrderType.STOP_LIMIT,
                volume = 0.1,
                lowerLimitPrice = 9000.0,
                lowerPrice = 9900.0,
                reservedVolume = 990.0
            )
        )
        testOrderBookWrapper.addStopLimitOrder(
            buildLimitOrder(
                uid = "4",
                clientId = "Client2",
                assetId = "BTCUSD",
                type = LimitOrderType.STOP_LIMIT,
                volume = 0.1,
                lowerLimitPrice = 10000.0,
                lowerPrice = 10900.0
            )
        )

        testBalanceHolderWrapper.updateBalance("trustedClient", "BTC", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("trustedClient", "BTC", 2.0, false)
        // negative reserved balance
        testBalanceHolderWrapper.updateBalance("trustedClient2", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("trustedClient2", "BTC", -0.001, false)

        testBalanceHolderWrapper.updateBalance("Client3", "BTC", 0.0)
        testBalanceHolderWrapper.updateReservedBalance("Client3", "BTC", -0.001)


        testBalanceHolderWrapper.updateBalance("Client1", "USD", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "USD", 1.0)

        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 2.0)

        testBalanceHolderWrapper.updateBalance("Client1", "EUR", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "EUR", 3.0)


        testBalanceHolderWrapper.updateBalance("Client2", "EUR", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "EUR", 0.0)

        testBalanceHolderWrapper.updateBalance("Client2", "BTC", 10.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "BTC", 1.0)

        testBalanceHolderWrapper.updateBalance("Client2", "USD", 990.0)
        testBalanceHolderWrapper.updateReservedBalance("Client2", "USD", 1.0)
    }

    @Test
    fun testRecalculate() {
        reservedVolumesRecalculator.recalculate()

        val testWalletDatabaseAccessor = balancesDatabaseAccessorsHolder.primaryAccessor as TestWalletDatabaseAccessor
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("trustedClient", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("trustedClient2", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client3", "BTC"))
        assertEquals(BigDecimal.valueOf(0.5), testWalletDatabaseAccessor.getReservedBalance("Client1", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client1", "USD"))
        assertEquals(BigDecimal.valueOf(0.7), testWalletDatabaseAccessor.getReservedBalance("Client1", "EUR"))
        assertEquals(BigDecimal.valueOf(1.0), testWalletDatabaseAccessor.getReservedBalance("Client2", "BTC"))
        assertEquals(BigDecimal.ZERO, testWalletDatabaseAccessor.getReservedBalance("Client2", "EUR"))
        assertEquals(BigDecimal.valueOf(2080.0), testWalletDatabaseAccessor.getReservedBalance("Client2", "USD"))

        assertEquals(7, reservedVolumesDatabaseAccessor.corrections.size)
        assertEquals(
            "1,2",
            reservedVolumesDatabaseAccessor.corrections.first {
                NumberUtils.equalsIgnoreScale(
                    it.newReserved,
                    BigDecimal.valueOf(0.7)
                )
            }.orderIds
        )
        assertEquals(
            "3,4",
            reservedVolumesDatabaseAccessor.corrections.first {
                NumberUtils.equalsIgnoreScale(
                    it.newReserved,
                    BigDecimal.valueOf(2080.0)
                )
            }.orderIds
        )

        assertEquals(7, clientsEventsQueue.size)
        assertEvent(false, "trustedClient", "BTC", "10", "2", "0", clientsEventsQueue)
        assertEvent(true, "trustedClient2", "BTC", "1", "-0.001", "0", clientsEventsQueue)
        assertEvent(true, "Client3", "BTC", "0", "-0.001", "0", clientsEventsQueue)
        assertEvent(false, "Client1", "BTC", "10", "2", "0.5", clientsEventsQueue)
        assertEvent(false, "Client1", "USD", "10", "1", "0", clientsEventsQueue)
        assertEvent(false, "Client1", "EUR", "10", "3", "0.7", clientsEventsQueue)
        assertEvent(true, "Client2", "USD", "990", "1", "2080", clientsEventsQueue)
    }

    private fun assertEvent(
        isCashIn: Boolean,
        clientId: String,
        assetId: String,
        balance: String,
        oldReserved: String,
        newReserved: String,
        events: Collection<Event<*>>
    ) {
        val event = events.single {
            isCashIn && it is CashInEvent && it.cashIn.walletId == clientId && it.cashIn.assetId == assetId
                    || !isCashIn && it is CashOutEvent && it.cashOut.walletId == clientId && it.cashOut.assetId == assetId
        }
        val message = "Client $clientId, assetId $assetId"
        val balanceUpdate = if (isCashIn) {
            event as CashInEvent
            assertEquals(1, event.balanceUpdates.size)
            event.balanceUpdates.first()
        } else {
            event as CashOutEvent
            assertEquals(1, event.balanceUpdates.size)
            event.balanceUpdates.first()
        }
        assertEquals(balance, balanceUpdate.oldBalance, message)
        assertEquals(balance, balanceUpdate.newBalance, message)
        assertEquals(oldReserved, balanceUpdate.oldReserved, message)
        assertEquals(newReserved, balanceUpdate.newReserved, message)
    }

}