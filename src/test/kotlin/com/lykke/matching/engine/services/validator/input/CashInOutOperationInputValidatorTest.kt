package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashInOutContextParser
import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashInOutOperationInputValidator
import com.lykke.matching.engine.utils.proto.createProtobufTimestampBuilder
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
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
import java.util.*
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (CashInOutOperationInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashInOutOperationInputValidatorTest {

    companion object {
        const val CLIENT_ID = "Client1"
        const val ASSET_ID = "USD"
    }

    @TestConfiguration
    class Config {
        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", ASSET_ID, 2))
            return testDictionariesDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var cashInOutOperationInputValidator: CashInOutOperationInputValidator

    @Autowired
    private lateinit var cashInOutContextInitializer: CashInOutContextParser

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CLIENT_ID, ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_ID, ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun assetDoesNotExist() {
        //given
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.assetId = "UNKNOWN"

        try {
            //when
            cashInOutOperationInputValidator
                .performValidation(getParsedData(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            //then
            assertEquals(ValidationException.Validation.UNKNOWN_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidFee() {
        //given
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()

        //when
        try {
            val fee = GrpcIncomingMessages.Fee.newBuilder()
                .setType(FeeType.EXTERNAL_FEE.externalId).build()
            cashInOutOperationBuilder.addFees(fee)

            val cashInOutContext = getParsedData(cashInOutOperationBuilder.build())
            cashInOutOperationInputValidator
                .performValidation(cashInOutContext)
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_FEE, e.validationType)
            throw e
        }

    }

    @Test(expected = ValidationException::class)
    fun testAssetEnabled() {
        //given
        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.DISABLED_ASSETS,
            ASSET_ID,
            ASSET_ID,
            true
        )

        //when
        try {
            val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
            cashInOutOperationBuilder.volume = "-1.0"
            cashInOutOperationInputValidator.performValidation(getParsedData(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testVolumeAccuracy() {
        //given
        val cashInOutOperationBuilder = getDefaultCashInOutOperationBuilder()
        cashInOutOperationBuilder.volume = "10.001"

        //when
        try {
            cashInOutOperationInputValidator.performValidation(getParsedData(cashInOutOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test
    fun validData() {
        cashInOutOperationInputValidator.performValidation(getParsedData(getDefaultCashInOutOperationBuilder().build()))
    }

    private fun getDefaultCashInOutOperationBuilder(): GrpcIncomingMessages.CashInOutOperation.Builder {
        return GrpcIncomingMessages.CashInOutOperation.newBuilder()
            .setId("test")
            .setWalletId(CLIENT_ID)
            .setAssetId(ASSET_ID)
            .setVolume("0.0")
            .setTimestamp(Date().createProtobufTimestampBuilder())
            .addFees(
                GrpcIncomingMessages.Fee.newBuilder()
                    .setType(FeeType.NO_FEE.externalId)
            )
    }

    private fun getMessageWrapper(cashInOutOperation: GrpcIncomingMessages.CashInOutOperation): CashInOutOperationMessageWrapper {
        return CashInOutOperationMessageWrapper(cashInOutOperation, TestStreamObserver())
    }

    private fun getParsedData(cashInOutOperation: GrpcIncomingMessages.CashInOutOperation): CashInOutParsedData {
        return cashInOutContextInitializer.parse(getMessageWrapper(cashInOutOperation))
    }
}