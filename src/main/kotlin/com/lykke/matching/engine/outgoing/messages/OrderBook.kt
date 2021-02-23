package com.lykke.matching.engine.outgoing.messages

import com.fasterxml.jackson.annotation.JsonProperty
import com.lykke.matching.engine.daos.LimitOrder
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.PriorityBlockingQueue

class OrderBook {
    val brokerId: String
    val assetPair: String

    @get:JsonProperty("isBuy")
    @JsonProperty("isBuy")
    val isBuy: Boolean

    val timestamp: Date

    val prices: MutableList<Order> = ArrayList()

    constructor(brokerId: String, assetPair: String, isBuy: Boolean, timestamp: Date) {
        this.brokerId = brokerId
        this.assetPair = assetPair
        this.isBuy = isBuy
        this.timestamp = timestamp
    }

    constructor(
        brokerId: String,
        assetPair: String,
        isBuy: Boolean,
        timestamp: Date,
        orders: PriorityBlockingQueue<LimitOrder>
    ) {
        this.brokerId = brokerId
        this.assetPair = assetPair
        this.isBuy = isBuy
        this.timestamp = timestamp

        while (!orders.isEmpty()) {
            val order = orders.poll()
            addVolumePrice(order.externalId, order.clientId, order.remainingVolume, order.price)
        }
    }

    fun addVolumePrice(id: String, clientId: String, volume: BigDecimal, price: BigDecimal) {
        prices.add(Order(id, clientId, volume, price))
    }
}

class Order(val id: String, val clientId: String, val volume: BigDecimal, val price: BigDecimal)
