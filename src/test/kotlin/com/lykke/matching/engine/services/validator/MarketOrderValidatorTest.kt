package com.lykke.matching.engine.services.validator

import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.*
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.daos.setting.AvailableSettingGroup
import com.lykke.matching.engine.database.TestDictionariesDatabaseAccessor
import com.lykke.matching.engine.database.TestSettingsDatabaseAccessor
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.MarketOrderValidator
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import com.lykke.matching.engine.utils.getSetting
import com.lykke.matching.engine.utils.proto.createProtobufTimestampBuilder
import com.lykke.matching.engine.utils.proto.toDate
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
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class), (MarketOrderValidatorTest.Config::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MarketOrderValidatorTest {

    companion object {
        const val CLIENT_NAME = "Client"
        const val OPERATION_ID = "test"
        const val ASSET_PAIR_ID = "EURUSD"
        const val BASE_ASSET_ID = "EUR"
        const val QUOTING_ASSET_ID = "USD"
    }

    @TestConfiguration
    class Config {

        @Bean
        @Primary
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAsset(Asset("", BASE_ASSET_ID, 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", QUOTING_ASSET_ID, 2))
            testDictionariesDatabaseAccessor.addAsset(Asset("", "BTC", 2))
            return testDictionariesDatabaseAccessor
        }

        @Bean
        fun testDictionariesDatabaseAccessor(): TestDictionariesDatabaseAccessor {
            val testDictionariesDatabaseAccessor = TestDictionariesDatabaseAccessor()
            testDictionariesDatabaseAccessor.addAssetPair(
                AssetPair(
                    ASSET_PAIR_ID,
                    BASE_ASSET_ID,
                    QUOTING_ASSET_ID,
                    2,
                    BigDecimal.valueOf(0.9)
                )
            )
            return testDictionariesDatabaseAccessor
        }

        @Bean
        @Primary
        fun test(): TestSettingsDatabaseAccessor {
            val testSettingsDatabaseAccessor = TestSettingsDatabaseAccessor()
            testSettingsDatabaseAccessor.createOrUpdateSetting(AvailableSettingGroup.DISABLED_ASSETS, getSetting("BTC"))
            return testSettingsDatabaseAccessor
        }
    }

    @Autowired
    private lateinit var marketOrderValidator: MarketOrderValidator

    @Autowired
    private lateinit var testDictionariesDatabaseAccessor: TestDictionariesDatabaseAccessor

    @Test(expected = OrderValidationException::class)
    fun testUnknownAsset() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.assetPairId = "BTCUSD"
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(
                order,
                getOrderBook(order.isBuySide()),
                listOf(NewFeeInstruction.create(getFeeInstruction()))
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.UnknownAsset, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testAssetDisabled() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.assetPairId = "BTCUSD"
        val order = toMarketOrder(marketOrderBuilder.build())
        testDictionariesDatabaseAccessor.addAssetPair(AssetPair("", "BTCUSD", "BTC", "USD", 2))

        //when
        try {
            marketOrderValidator.performValidation(
                order,
                getOrderBook(order.isBuySide()),
                listOf(NewFeeInstruction.create(getFeeInstruction()))
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.DisabledAsset, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolume() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.volume = "0.1"
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(
                order,
                getOrderBook(order.isBuySide()),
                listOf(NewFeeInstruction.create(getFeeInstruction()))
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.TooSmallVolume, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun invalidOrderBook() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(
                order, AssetOrderBook(ASSET_PAIR_ID).getOrderBook(order.isBuySide()),
                listOf(NewFeeInstruction.create(getFeeInstruction()))
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.NoLiquidity, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidVolumeAccuracy() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        marketOrderBuilder.volume = "1.1111"
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        try {
            marketOrderValidator.performValidation(
                order,
                getOrderBook(order.isBuySide()),
                listOf(NewFeeInstruction.create(getFeeInstruction()))
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidVolumeAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testInvalidPriceAccuracy() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())
        order.price = BigDecimal.valueOf(1.1111)

        //when
        try {
            marketOrderValidator.performValidation(
                order,
                getOrderBook(order.isBuySide()),
                listOf(NewFeeInstruction.create(getFeeInstruction()))
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.InvalidPriceAccuracy, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidData() {
        //given
        val marketOrderBuilder = getDefaultMarketOrderBuilder()
        val order = toMarketOrder(marketOrderBuilder.build())

        //when
        marketOrderValidator.performValidation(
            order,
            getOrderBook(order.isBuySide()),
            listOf(NewFeeInstruction.create(getFeeInstruction()))
        )
    }

    private fun toMarketOrder(message: GrpcIncomingMessages.MarketOrder): MarketOrder {
        val now = Date()
        return MarketOrder(
            UUID.randomUUID().toString(),
            message.id,
            message.assetPairId,
            "",
            "",
            message.walletId,
            BigDecimal(message.volume),
            null,
            OrderStatus.Processing.name,
            now,
            message.timestamp.toDate(),
            now,
            null,
            message.straight,
            if (message.hasReservedLimitVolume()) BigDecimal(message.reservedLimitVolume.value) else null,
            NewFeeInstruction.create(message.feesList)
        )
    }

    private fun getOrderBook(isBuy: Boolean): PriorityBlockingQueue<LimitOrder> {
        val assetOrderBook = AssetOrderBook(ASSET_PAIR_ID)
        val now = Date()
        assetOrderBook.addOrder(
            LimitOrder(
                "test", "test",
                ASSET_PAIR_ID, "", "", CLIENT_NAME, BigDecimal.valueOf(1.0), BigDecimal.valueOf(1.0),
                OrderStatus.InOrderBook.name, now, now, now, BigDecimal.valueOf(1.0), now, BigDecimal.valueOf(1.0),
                null, null, null, null, null, null, null, null,
                null, null, null
            )
        )

        return assetOrderBook.getOrderBook(isBuy)
    }

    private fun getDefaultMarketOrderBuilder(): GrpcIncomingMessages.MarketOrder.Builder {
        return GrpcIncomingMessages.MarketOrder.newBuilder()
            .setId(OPERATION_ID)
            .setAssetPairId("EURUSD")
            .setTimestamp(Date().createProtobufTimestampBuilder())
            .setWalletId(CLIENT_NAME)
            .setVolume("1.0")
            .setStraight(true)
            .addFees(getFeeInstruction())
    }

    private fun getFeeInstruction(): GrpcIncomingMessages.Fee {
        return GrpcIncomingMessages.Fee.newBuilder()
            .setType(FeeType.NO_FEE.externalId)
            .build()
    }
}