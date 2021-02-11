package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper
import com.lykke.matching.engine.services.validator.input.CashInOutOperationInputValidatorTest
import com.lykke.matching.engine.services.validators.business.CashInOutOperationBusinessValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashInOutOperationBusinessValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashInOutOperationBusinessValidatorTest {

    companion object {
        const val CLIENT_NAME = "Client1"
        const val ASSET_ID = "USD"
    }

    @TestConfiguration
    class Config {

        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", CashInOutOperationInputValidatorTest.ASSET_ID, 2))
            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var cashInOutOperationBusinessValidator: CashInOutOperationBusinessValidator

    @Autowired
    private lateinit var cashInOutContextInitializer: CashInOutContextParser

    @Test(expected = ValidationException::class)
    fun testBalanceValid() {
        //given
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME, ASSET_ID, 500.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME, ASSET_ID, 250.0)
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.volume = "-300.0"

        //when
        try {
            cashInOutOperationBusinessValidator.performValidation(getContext(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    private fun getDefaultCashInOutOperationBuilder(): GrpcIncomingMessages.CashInOutOperation.Builder {
        return GrpcIncomingMessages.CashInOutOperation.newBuilder()
            .setId("test")
            .setWalletId(CLIENT_NAME)
            .setAssetId(ASSET_ID)
            .setVolume("0.0")
            .setTimestamp(Date().createProtobufTimestampBuilder())
            .addFees(
                GrpcIncomingMessages.Fee.newBuilder()
                    .setType(FeeType.NO_FEE.externalId)
            )
    }

    private fun getMessageWrapper(cashInOutOperation: GrpcIncomingMessages.CashInOutOperation): CashInOutOperationMessageWrapper {
        return CashInOutOperationMessageWrapper(
            cashInOutOperation,
            TestStreamObserver()
        )
    }

    private fun getContext(cashInOutOperation: GrpcIncomingMessages.CashInOutOperation): CashInOutContext {
        return (cashInOutContextInitializer.parse(getMessageWrapper(cashInOutOperation)).messageWrapper as CashInOutOperationMessageWrapper).context as CashInOutContext
    }
}