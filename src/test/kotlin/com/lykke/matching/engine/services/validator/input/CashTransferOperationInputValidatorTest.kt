package com.lykke.matching.engine.services.validator.input

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.cache.ApplicationSettingsCache
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData
import com.lykke.matching.engine.incoming.parsers.impl.CashTransferContextParser
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashTransferOperationInputValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (CashTransferOperationInputValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CashTransferOperationInputValidatorTest {

    companion object {
        const val CLIENT_NAME1 = "Client1"
        const val CLIENT_NAME2 = "Client2"
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
    private lateinit var cashTransferContextParser: CashTransferContextParser

    @Autowired
    private lateinit var applicationSettingsCache: ApplicationSettingsCache

    @Autowired
    private lateinit var cashTransferOperationInputValidator: CashTransferOperationInputValidator

    @Autowired
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Before
    fun setUp() {
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME1, ASSET_ID, 1000.0)
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME1, ASSET_ID, 50.0)
    }

    @Test(expected = ValidationException::class)
    fun testAssetExists() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.assetId = "UNKNOWN"

        try {
            //when
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.UNKNOWN_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testAssetEnabled() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        applicationSettingsCache.createOrUpdateSettingValue(
            AvailableSettingGroup.DISABLED_ASSETS,
            ASSET_ID,
            ASSET_ID,
            true
        )
        cashTransferOperationBuilder.volume = "-1.0"

        //when
        try {
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.DISABLED_ASSET, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testInvalidFee() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()

        //when
        try {
            val invalidFee = GrpcIncomingMessages.Fee.newBuilder()
                .setType(FeeType.EXTERNAL_FEE.externalId).build()


            cashTransferOperationBuilder.addFees(invalidFee)
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_FEE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testVolumeAccuracy() {
        //given
        val cashTransferOperationBuilder = getCashTransferOperationBuilder()
        cashTransferOperationBuilder.volume = "10.001"

        //when
        try {
            cashTransferOperationInputValidator.performValidation(getParsedData(cashTransferOperationBuilder.build()))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //when
        cashTransferOperationInputValidator.performValidation(getParsedData(getCashTransferOperationBuilder().build()))
    }

    fun getCashTransferOperationBuilder(): GrpcIncomingMessages.CashTransferOperation.Builder {
        return GrpcIncomingMessages.CashTransferOperation
            .newBuilder()
            .setId("test")
            .setAssetId(ASSET_ID)
            .setTimestamp(Date().createProtobufTimestampBuilder())
            .setFromWalletId(CLIENT_NAME1)
            .setToWalletId(CLIENT_NAME2).setVolume("0.0")
    }

    private fun getMessageWrapper(message: GrpcIncomingMessages.CashTransferOperation): CashTransferOperationMessageWrapper {
        return CashTransferOperationMessageWrapper(message, TestStreamObserver())
    }

    private fun getParsedData(message: GrpcIncomingMessages.CashTransferOperation): CashTransferParsedData {
        return cashTransferContextParser.parse(getMessageWrapper(message))
    }
}
