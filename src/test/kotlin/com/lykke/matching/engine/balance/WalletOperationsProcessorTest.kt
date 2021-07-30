package com.lykke.matching.engine.balance

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.DictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.utils.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (WalletOperationsProcessorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WalletOperationsProcessorTest : AbstractTest() {

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): DictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 8))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "ETH", 6))

            return testDictionariesDatabaseAccessor
        }
    }

    @Test
    fun testPreProcessWalletOperations() {
        testBalanceHolderWrapper.updateBalance("Client1", "BTC", 1.0)
        testBalanceHolderWrapper.updateReservedBalance("Client1", "BTC", 0.1)
        initServices()

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null)

        walletOperationsProcessor.preProcess(
            listOf(
                WalletOperation("", "", "Client1", "BTC", BigDecimal.valueOf(-0.5), BigDecimal.valueOf(-0.1)),
                WalletOperation("", "", "Client2", "ETH", BigDecimal.valueOf(2.0), BigDecimal.valueOf(0.1))

            )
        )

        walletOperationsProcessor.preProcess(
            listOf(WalletOperation("", "", "Client2", "ETH", BigDecimal.valueOf(1.0), BigDecimal.valueOf(0.2)))
        )

        assertFailsWith(BalanceException::class) {
            walletOperationsProcessor.preProcess(
                listOf(
                    WalletOperation("", "", "Client1", "BTC", ZERO, BigDecimal.valueOf(-0.1)),
                    WalletOperation("", "", "Client3", "BTC", BigDecimal.valueOf(1.0), ZERO)
                )
            )
        }
        assertTrue(walletOperationsProcessor.persistBalances(null, null, null, null, null))
        walletOperationsProcessor.apply()

        assertBalance("Client1", "BTC", 0.5, 0.0)
        assertBalance("Client2", "ETH", 3.0, 0.3)
        assertBalance("Client3", "BTC", 0.0, 0.0)

//        assertEquals(1, balanceUpdateHandlerTest.getCountOfBalanceUpdate())
//        val balanceUpdate = balanceUpdateHandlerTest.balanceUpdateQueue.poll() as BalanceUpdate
//        assertEquals(2, balanceUpdate.balances.size)
//        assertEquals("id", balanceUpdate.id)
//        assertEquals("type", balanceUpdate.type)
//
//        val clientBalanceUpdate1 = balanceUpdate.balances.first { it.walletId == "Client1" }
//        assertNotNull(clientBalanceUpdate1)
//        assertEquals("BTC", clientBalanceUpdate1.asset)
//        assertEquals(BigDecimal.valueOf(1.0), clientBalanceUpdate1.oldBalance)
//        assertEquals(BigDecimal.valueOf(0.5), clientBalanceUpdate1.newBalance)
//        assertEquals(BigDecimal.valueOf(0.1), clientBalanceUpdate1.oldReserved)
//        assertEquals(ZERO, clientBalanceUpdate1.newReserved)
//
//        val clientBalanceUpdate2 = balanceUpdate.balances.first { it.walletId == "Client2" }
//        assertNotNull(clientBalanceUpdate2)
//        assertEquals("ETH", clientBalanceUpdate2.asset)
//        assertEquals(ZERO, clientBalanceUpdate2.oldBalance)
//        assertEquals(BigDecimal.valueOf(3.0), clientBalanceUpdate2.newBalance)
//        assertEquals(ZERO, clientBalanceUpdate2.oldReserved)
//        assertEquals(BigDecimal.valueOf(0.3), clientBalanceUpdate2.newReserved)
    }

    @Test
    fun testForceProcessInvalidWalletOperations() {
        initServices()

        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null)

        walletOperationsProcessor.preProcess(
            listOf(
                WalletOperation("", "", "Client1", "BTC", ZERO, BigDecimal.valueOf(-0.1))
            ), true
        )

        assertTrue(walletOperationsProcessor.persistBalances(null, null, null, null, null))
        walletOperationsProcessor.apply()

        assertBalance("Client1", "BTC", 0.0, -0.1)
    }

    @Test
    fun testValidation() {
        assertTrue(validate("Client", "Asset", 0.0, 0.0, 1.0, 0.0))
        assertTrue(validate("Client", "Asset", -1.0, 0.0, -1.0, 0.0))
        assertTrue(validate("Client", "Asset", 0.0, -1.0, 0.0, -1.0))
        assertTrue(validate("Client", "Asset", 0.0, -1.0, 0.0, -0.9))
        assertTrue(validate("Client", "Asset", 0.0, -1.0, 0.2, -1.0))
        assertTrue(validate("Client", "Asset", 1.0, 2.0, 1.0, 2.0))
        assertTrue(validate("Client", "Asset", 1.0, 2.0, 1.0, 1.9))
        assertTrue(validate("Client", "Asset", 0.05, 0.09, 0.0, 0.04))

        assertFalse(validate("Client", "Asset", 0.0, 0.0, -1.0, -1.1))
        assertFalse(validate("Client", "Asset", 0.0, 0.0, -1.0, 0.0))
        assertFalse(validate("Client", "Asset", -1.0, 0.0, -1.1, -0.1))
        assertFalse(validate("Client", "Asset", 0.0, 0.0, 0.0, -1.0))
        assertFalse(validate("Client", "Asset", 0.0, -1.0, -0.1, -1.0))
        assertFalse(validate("Client", "Asset", 0.0, -1.0, 0.0, -1.1))
        assertFalse(validate("Client", "Asset", 0.0, 0.0, 1.0, 2.0))
        assertFalse(validate("Client", "Asset", 1.0, 2.0, 1.0, 2.1))
        assertFalse(validate("Client", "Asset", 1.0, 2.0, -0.1, 0.9))
    }

    @Test
    fun testTrustedClientReservedOperations() {
        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.TRUSTED_CLIENTS,
            "TrustedClient1",
            "TrustedClient1",
            true
        )
        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.TRUSTED_CLIENTS,
            "TrustedClient2",
            "TrustedClient2",
            true
        )

        testBalanceHolderWrapper.updateBalance("TrustedClient1", "BTC", 1.0)
        testBalanceHolderWrapper.updateBalance("TrustedClient2", "EUR", 1.0)
        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null)

        walletOperationsProcessor.preProcess(
            listOf(
                WalletOperation("", "", "TrustedClient1", "BTC", ZERO, BigDecimal.valueOf(0.1)),
                WalletOperation("", "", "TrustedClient2", "ETH", BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1))
            )
        )

        val clientBalanceUpdates = walletOperationsProcessor.getClientBalanceUpdates()
        assertEquals(1, clientBalanceUpdates.size)
        assertEquals("ETH", clientBalanceUpdates.single().asset)
        assertEquals("TrustedClient2", clientBalanceUpdates.single().walletId)
        assertEquals(BigDecimal.valueOf(0.1), clientBalanceUpdates.single().newBalance)
        assertEquals(ZERO, clientBalanceUpdates.single().newReserved)
    }

    @Test
    fun testNotChangedBalance() {
        val walletOperationsProcessor = balancesHolder.createWalletProcessor(null)

        walletOperationsProcessor.preProcess(
            listOf(
                WalletOperation("", "", "Client1", "BTC", BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1)),
                WalletOperation("", "", "Client1", "BTC", BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1)),
                WalletOperation("", "", "Client2", "BTC", BigDecimal.valueOf(0.00000001), ZERO),
                WalletOperation(
                    "",
                    "",
                    "Client2",
                    "BTC",
                    BigDecimal.valueOf(0.00000001),
                    BigDecimal.valueOf(0.00000001)
                ),
                WalletOperation(
                    "",
                    "",
                    "Client2",
                    "BTC",
                    BigDecimal.valueOf(-0.00000002),
                    BigDecimal.valueOf(-0.00000001)
                ),
                WalletOperation("", "", "Client3", "BTC", BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.1))
            )
        )

        walletOperationsProcessor.preProcess(
            listOf(
                WalletOperation("", "", "Client3", "BTC", BigDecimal.valueOf(-0.1), BigDecimal.valueOf(-0.1))
            )
        )

        val clientBalanceUpdates = walletOperationsProcessor.getClientBalanceUpdates()
        assertEquals(1, clientBalanceUpdates.size)
        assertEquals("BTC", clientBalanceUpdates.single().asset)
        assertEquals("Client1", clientBalanceUpdates.single().walletId)
        assertEquals(ZERO, clientBalanceUpdates.single().oldBalance)
        assertEquals(ZERO, clientBalanceUpdates.single().oldReserved)
        assertEquals(BigDecimal.valueOf(0.2), clientBalanceUpdates.single().newBalance)
        assertEquals(BigDecimal.valueOf(0.2), clientBalanceUpdates.single().newReserved)
    }

    private fun validate(
        @Suppress("SameParameterValue") clientId: String,
        @Suppress("SameParameterValue") assetId: String,
        oldBalance: Double,
        oldReserved: Double,
        newBalance: Double,
        newReserved: Double
    ): Boolean {
        return try {
            validateBalanceChange(
                clientId, assetId, BigDecimal.valueOf(oldBalance), BigDecimal.valueOf(oldReserved), ZERO,
                BigDecimal.valueOf(newBalance), BigDecimal.valueOf(newReserved), ZERO
            )
            true
        } catch (e: BalanceException) {
            false
        }
    }

    private fun assertBalance(clientId: String, assetId: String, balance: Double, reserved: Double) {
        assertEquals(BigDecimal.valueOf(balance), balancesHolder.getBalance(clientId, assetId))
        assertEquals(
            BigDecimal.valueOf(reserved),
            balancesHolder.getReservedForOrdersBalance("", "", clientId, assetId)
        )
        assertEquals(BigDecimal.valueOf(balance), testWalletDatabaseAccessor.getBalance(clientId, assetId))
        assertEquals(BigDecimal.valueOf(reserved), testWalletDatabaseAccessor.getReservedBalance(clientId, assetId))
    }
}