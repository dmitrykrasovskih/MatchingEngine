package com.lykke.matching.engine.database.cache

import com.lykke.matching.engine.daos.TickUpdateInterval
import com.lykke.matching.engine.database.HistoryTicksDatabaseAccessor
import com.lykke.matching.engine.history.TickBlobHolder
import com.nhaarman.mockito_kotlin.eq
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.junit.MockitoJUnitRunner
import java.math.BigDecimal
import java.util.*

@RunWith(MockitoJUnitRunner::class)
class TestMarketStateCache {

    @Mock
    private lateinit var historyDatabaseAccessor: HistoryTicksDatabaseAccessor

    private lateinit var marketStateCache: MarketStateCache

    @Before
    fun init() {
        marketStateCache = MarketStateCache(historyDatabaseAccessor, 4000L)
    }

    @Test
    fun testAddDataToExistingTick() {
        //given
        val chfUsdTick = getOneHourTickHolder(
            "CHFUSD", LinkedList(listOf(BigDecimal.valueOf(0.3))),
            LinkedList(listOf(BigDecimal.valueOf(0.3)))
        )
        @Suppress("SameParameterValue") val usdBtcTick = getOneHourTickHolder(
            "USDBTC", LinkedList(listOf(BigDecimal.valueOf(0.9))),
            LinkedList(listOf(BigDecimal.valueOf(0.8)))
        )
        Mockito.`when`(historyDatabaseAccessor.loadHistoryTicks())
            .thenReturn(
                listOf(
                    chfUsdTick,
                    usdBtcTick
                )
            )

        //when
        marketStateCache.refresh()

        Thread.sleep(1000)

        val currentUpdateTime = System.currentTimeMillis()
        marketStateCache.addTick("CHFUSD", BigDecimal.valueOf(0.5), BigDecimal.valueOf(0.7), currentUpdateTime)
        marketStateCache.flush()

        //then
        val expectedTick = getOneHourTickHolder(
            "CHFUSD", LinkedList(listOf(BigDecimal.valueOf(0.3))),
            LinkedList(listOf(BigDecimal.valueOf(0.3)))
        )
        expectedTick.addPrice(BigDecimal.valueOf(0.5), BigDecimal.valueOf(0.7), currentUpdateTime)

        verify(historyDatabaseAccessor).saveHistoryTick(eq(expectedTick))
    }

    @Test
    fun testDataShouldNotBeAddedToTickThatIsNotTimeFor() {
        //given
        val chfUsdTick = getOneHourTickHolder(
            "CHFUSD",
            LinkedList(listOf(BigDecimal.valueOf(0.3))),
            LinkedList(listOf(BigDecimal.valueOf(0.3)))
        )
        val usdBtcTick = getOneDayTickHolder(
            "USDBTC",
            LinkedList(listOf(BigDecimal.valueOf(0.9))),
            LinkedList(listOf(BigDecimal.valueOf(0.8)))
        )
        Mockito.`when`(historyDatabaseAccessor.loadHistoryTicks())
            .thenReturn(
                listOf(
                    chfUsdTick,
                    usdBtcTick
                )
            )

        //when
        marketStateCache.refresh()

        Thread.sleep(1000)

        val currentUpdateTime = System.currentTimeMillis()
        marketStateCache.addTick("CHFUSD", BigDecimal.valueOf(0.5), BigDecimal.valueOf(0.7), currentUpdateTime)
        marketStateCache.addTick("USDBTC", BigDecimal.valueOf(0.7), BigDecimal.valueOf(0.4), currentUpdateTime)
        marketStateCache.flush()

        val expectedTick = getOneHourTickHolder(
            "CHFUSD", LinkedList(listOf(BigDecimal.valueOf(0.3))),
            LinkedList(listOf(BigDecimal.valueOf(0.3)))
        )
        expectedTick.addPrice(BigDecimal.valueOf(0.5), BigDecimal.valueOf(0.7), currentUpdateTime)

        //then
        verify(historyDatabaseAccessor).saveHistoryTick(eq(expectedTick))
    }

    @Test
    fun testAddDataToNonExistingTick() {
        //given
        val chfUsdTick = TickBlobHolder(
            assetPair = "CHFUSD",
            tickUpdateInterval = TickUpdateInterval.ONE_HOUR,
            ask = BigDecimal.valueOf(0.1),
            bid = BigDecimal.valueOf(0.2),
            lastUpdate = System.currentTimeMillis(),
            frequency = 4000L
        )

        //when
        marketStateCache.addTick(
            chfUsdTick.assetPair,
            chfUsdTick.askTicks.first,
            chfUsdTick.bidTicks.first,
            chfUsdTick.lastUpdate
        )
        marketStateCache.flush()

        //then
        verify(historyDatabaseAccessor).saveHistoryTick(eq(chfUsdTick))
        verify(historyDatabaseAccessor).loadHistoryTick(eq(chfUsdTick.assetPair), eq(chfUsdTick.tickUpdateInterval))
    }

    private fun getOneHourTickHolder(
        assetPair: String, ask: LinkedList<BigDecimal>, bid: LinkedList<BigDecimal>,
        updateTime: Long = System.currentTimeMillis()
    ): TickBlobHolder {
        return TickBlobHolder(
            assetPair = assetPair,
            tickUpdateInterval = TickUpdateInterval.ONE_HOUR,
            askTicks = ask,
            bidTicks = bid,
            lastUpdate = updateTime,
            frequency = 4000L
        )
    }

    private fun getOneDayTickHolder(
        @Suppress("SameParameterValue") assetPair: String,
        ask: LinkedList<BigDecimal>,
        bid: LinkedList<BigDecimal>
    ): TickBlobHolder {
        return TickBlobHolder(
            assetPair = assetPair,
            tickUpdateInterval = TickUpdateInterval.ONE_DAY,
            askTicks = ask,
            bidTicks = bid,
            lastUpdate = System.currentTimeMillis(),
            frequency = 4000L
        )
    }
}