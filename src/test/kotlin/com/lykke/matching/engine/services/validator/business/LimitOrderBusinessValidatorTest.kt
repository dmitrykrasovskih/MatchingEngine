package com.lykke.matching.engine.services.validator.business

import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.services.AssetOrderBook
import com.lykke.matching.engine.services.validators.business.impl.LimitOrderBusinessValidatorImpl
import com.lykke.matching.engine.services.validators.impl.OrderValidationException
import org.junit.Assert.assertEquals
import org.junit.Test
import java.math.BigDecimal
import java.util.*

class LimitOrderBusinessValidatorTest {
    private companion object {
        private const val ASSET_PAIR_ID = "BTCUSD"
    }

    @Test(expected = OrderValidationException::class)
    fun testLeadToNegativeSpread() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        try {
            //when
            val orderBook = AssetOrderBook(ASSET_PAIR_ID)

            orderBook.addOrder(getLimitOrder(fees = listOf(getValidFee()), volume = BigDecimal.valueOf(-1.0)))
            limitOrderBusinessValidatorImpl.performValidation(
                true,
                getLimitOrder(fees = listOf(getValidFee())),
                BigDecimal.valueOf(10.0),
                BigDecimal.valueOf(9.0),
                orderBook,
                Date()
            )
        } catch (e: OrderValidationException) {
            assertEquals(OrderStatus.LeadToNegativeSpread, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testPreviousOrderNotFount() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(
                true,
                getLimitOrder(status = OrderStatus.NotFoundPrevious.name, fees = listOf(getValidFee())),
                BigDecimal.valueOf(12.0),
                BigDecimal.valueOf(11.0),
                getValidOrderBook(),
                Date()
            )
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.NotFoundPrevious, e.orderStatus)
            throw e
        }
    }

    @Test(expected = OrderValidationException::class)
    fun testNotEnoughFounds() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        try {
            //when
            limitOrderBusinessValidatorImpl.performValidation(
                true,
                getLimitOrder(status = OrderStatus.NotEnoughFunds.name, fees = listOf(getValidFee())),
                BigDecimal.valueOf(12.0),
                BigDecimal.valueOf(11.0),
                getValidOrderBook(),
                Date()
            )
        } catch (e: OrderValidationException) {
            //then
            assertEquals(OrderStatus.NotEnoughFunds, e.orderStatus)
            throw e
        }
    }

    @Test
    fun testValidOrder() {
        //given
        val limitOrderBusinessValidatorImpl = LimitOrderBusinessValidatorImpl()

        //when
        limitOrderBusinessValidatorImpl.performValidation(
            true,
            getLimitOrder(fees = listOf(getValidFee())),
            BigDecimal.valueOf(12.0),
            BigDecimal.valueOf(11.0),
            getValidOrderBook(),
            Date()
        )
    }

    private fun getLimitOrder(
        fees: List<NewLimitOrderFeeInstruction>? = null,
        assetPair: String = ASSET_PAIR_ID,
        price: BigDecimal = BigDecimal.valueOf(1.0),
        volume: BigDecimal = BigDecimal.valueOf(1.0),
        status: String = OrderStatus.InOrderBook.name
    ): LimitOrder {
        return LimitOrder(
            "test",
            "test",
            assetPair,
            "",
            "",
            "test",
            volume,
            price,
            status,
            Date(),
            Date(),
            Date(),
            BigDecimal.valueOf(1.0),
            null,
            expiryTime = null,
            timeInForce = null,
            type = LimitOrderType.LIMIT,
            fees = fees,
            lowerLimitPrice = null,
            lowerPrice = null,
            upperLimitPrice = null,
            upperPrice = null,
            previousExternalId = null,
            parentOrderExternalId = null,
            childOrderExternalId = null
        )
    }

    fun getValidFee(): NewLimitOrderFeeInstruction {
        return NewLimitOrderFeeInstruction(FeeType.NO_FEE, null, null, null, null, null, null, emptyList(), null)
    }

    private fun getValidOrderBook(): AssetOrderBook {
        return AssetOrderBook(ASSET_PAIR_ID)
    }
}