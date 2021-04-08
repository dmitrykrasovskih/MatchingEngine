package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.balance.util.TestBalanceHolderWrapper
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.context.ReservedCashInOutContext
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.incoming.parsers.data.ReservedCashInOutParsedData
import com.lykke.matching.engine.incoming.parsers.impl.ReservedCashInOutContextParser
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.lykke.matching.engine.services.validators.business.ReservedCashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.ReservedCashInOutOperationValidator
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
@SpringBootTest(classes = [(TestApplicationContext::class), (ReservedCashInOutOperationBusinessValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ReservedCashInOutOperationBusinessValidatorTest {

    companion object {
        const val CLIENT_NAME = "Client"
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
    private lateinit var testBalanceHolderWrapper: TestBalanceHolderWrapper

    @Autowired
    private lateinit var reservedCashInOutOperationValidator: ReservedCashInOutOperationValidator

    @Autowired
    private lateinit var reservedCashInOutOperationBusinessValidator: ReservedCashInOutOperationBusinessValidator

    @Autowired
    private lateinit var reservedCashInOutContextParser: ReservedCashInOutContextParser

    @Before
    fun int() {
        testBalanceHolderWrapper.updateReservedBalance(CLIENT_NAME, ASSET_ID, 500.0)
        testBalanceHolderWrapper.updateBalance(CLIENT_NAME, ASSET_ID, 550.0)
    }

    @Test(expected = ValidationException::class)
    fun testVolumeAccuracyInvalid() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
            .setReservedVolume("1.111")
            .build()

        //when
        try {
            reservedCashInOutOperationValidator.performValidation(getParsedData(message))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.INVALID_VOLUME_ACCURACY, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testBalanceInvalid() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
            .setReservedVolume("-550.0")
            .build()


        //when
        try {
            reservedCashInOutOperationBusinessValidator.performValidation(getContext(message))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.LOW_BALANCE, e.validationType)
            throw e
        }
    }

    @Test(expected = ValidationException::class)
    fun testReservedBalanceInvalid() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
            .setReservedVolume("51.0")
            .build()

        //when
        try {
            reservedCashInOutOperationBusinessValidator.performValidation(getContext(message))
        } catch (e: ValidationException) {
            assertEquals(ValidationException.Validation.RESERVED_VOLUME_HIGHER_THAN_BALANCE, e.validationType)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //given
        val message = getDefaultReservedOperationMessageBuilder()
            .build()

        //when
        reservedCashInOutOperationValidator.performValidation(getParsedData(message))
    }


    private fun getDefaultReservedOperationMessageBuilder(): GrpcIncomingMessages.ReservedCashInOutOperation.Builder {
        return GrpcIncomingMessages.ReservedCashInOutOperation.newBuilder()
            .setId("test")
            .setWalletId(CLIENT_NAME)
            .setTimestamp(Date().createProtobufTimestampBuilder())
            .setAssetId(ASSET_ID)
            .setReservedVolume("0.0")
    }


    private fun getContext(cashInOutOperation: GrpcIncomingMessages.ReservedCashInOutOperation): ReservedCashInOutContext {
        return (reservedCashInOutContextParser.parse(getMessageWrapper(cashInOutOperation)).messageWrapper as ReservedCashInOutOperationMessageWrapper).context as ReservedCashInOutContext
    }

    private fun getMessageWrapper(cashInOutOperation: GrpcIncomingMessages.ReservedCashInOutOperation): ReservedCashInOutOperationMessageWrapper {
        return ReservedCashInOutOperationMessageWrapper(cashInOutOperation, TestStreamObserver())
    }

    private fun getParsedData(cashInOutOperation: GrpcIncomingMessages.ReservedCashInOutOperation): ReservedCashInOutParsedData {
        return reservedCashInOutContextParser.parse(getMessageWrapper(cashInOutOperation))
    }
}