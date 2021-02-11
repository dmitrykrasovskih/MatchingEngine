package com.lykke.matching.engine.services.validator.business

import com.google.protobuf.StringValue
import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.notification.BalanceUpdateHandlerTest
import com.lykke.matching.engine.services.validators.business.CashTransferOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.proto.createProtobufTimestampBuilder
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashTransferOperationBusinessValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashTransferOperationBusinessValidatorTest {

    companion object {
        const val CLIENT_NAME1 = "Client1"
        const val CLIENT_NAME2 = "Client2"
        const val ASSET_ID = "USD"
    }

    @Autowired
    private lateinit var cashTransferOperationBusinessValidator: CashTransferOperationBusinessValidator

    @Autowired
    private lateinit var cashTransferContextParser: CashTransferContextParser

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", ASSET_ID, 2))
            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        fun testBalanceHolderWrapper(
            balanceUpdateHandlerTest: BalanceUpdateHandlerTest,
            balancesHolder: BalancesHolder
        ): TestBalanceHolderWrapper {
            val testBalanceHolderWrapper = TestBalanceHolderWrapper(balanceUpdateHandlerTest, balancesHolder)
            testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID, 100.0)
            testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME1, ASSET_ID, 50.0)
            return testBalanceHolderWrapper
        }
    }

    @Test
    fun testLowBalanceHighOverdraftLimit() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.overdraftLimit = StringValue.of("40.0")
        cashTransferOperationBuilder.volume = "60.0"

        //when
        cashTransferOperationBusinessValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
    }

    @Test(expected = ValidationException::class)
    fun testLowBalance() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.volume = "60.0"
        cashTransferOperationBuilder.overdraftLimit = StringValue.of("0.0")

        //when
        try {
            cashTransferOperationBusinessValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test
    fun testPositiveOverdraftLimit() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.volume = "30.0"
        cashTransferOperationBuilder.overdraftLimit = StringValue.of("1.0")

        //when
        cashTransferOperationBusinessValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
    }

    @Test(expected = ValidationException::class)
    fun testNegativeOverdraftLimit() {
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.volume = "60.0"
        cashTransferOperationBuilder.overdraftLimit = StringValue.of("-1.0")

        //when
        try {
            cashTransferOperationBusinessValidator.performValidation(getContext(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }


    fun getCashTransferOperationBuilder(): GrpcIncomingMessages.CashTransferOperation.Builder {
        return GrpcIncomingMessages.CashTransferOperation
            .newBuilder()
            .setId("test")
            .setAssetId(ASSET_ID)
            .setTimestamp(Date().createProtobufTimestampBuilder())
            .setFromWalletId(CLIENT_NAME1)
            .setToWalletId(CLIENT_NAME2).setVolume("10.0")
    }

    private fun getMessageWrapper(message: GrpcIncomingMessages.CashTransferOperation): CashTransferOperationMessageWrapper {
        return CashTransferOperationMessageWrapper(message, TestStreamObserver())
    }

    private fun getContext(message: GrpcIncomingMessages.CashTransferOperation): CashTransferContext {
        return (cashTransferContextParser.parse(getMessageWrapper(message)).messageWrapper as CashTransferOperationMessageWrapper).context as CashTransferContext
    }
}