package com.lykke.matching.engine.database

import com.lykke.matching.engine.daos.LkkTrade
import java.util.*

class TestMarketOrderDatabaseAccessor : MarketOrderDatabaseAccessor {

    val lkkTrades = ArrayList<LkkTrade>()

    override fun addLkkTrades(trades: List<LkkTrade>) {
        this.lkkTrades.addAll(trades)
    }
}