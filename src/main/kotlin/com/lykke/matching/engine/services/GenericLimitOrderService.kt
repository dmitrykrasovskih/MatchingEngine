package com.lykke.matching.engine.services

import com.lykke.matching.engine.daos.BestPrice
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.holders.OrdersDatabaseAccessorsHolder
import com.lykke.matching.engine.order.ExpiryOrdersQueue
import com.lykke.matching.engine.order.OrderStatus
import com.lykke.matching.engine.order.OrderStatus.Cancelled
import com.lykke.matching.engine.order.transaction.CurrentTransactionOrderBooksHolder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@Component
class GenericLimitOrderService @Autowired constructor(
    private val orderBookDatabaseAccessorHolder: OrdersDatabaseAccessorsHolder,
    private val expiryOrdersQueue: ExpiryOrdersQueue
) : AbstractGenericLimitOrderService<AssetOrderBook> {

    //asset -> orderBook
    private val limitOrdersQueues = ConcurrentHashMap<String, AssetOrderBook>()
    private val limitOrdersMap = HashMap<String, LimitOrder>()
    private val clientLimitOrdersMap = HashMap<String, MutableList<LimitOrder>>()
    var initialOrdersCount = 0

    init {
        update()
    }

    final fun update() {
        limitOrdersMap.values.forEach {
            expiryOrdersQueue.removeIfOrderHasExpiryTime(it)
        }
        limitOrdersQueues.clear()
        limitOrdersMap.clear()
        clientLimitOrdersMap.clear()
        val orders = orderBookDatabaseAccessorHolder.primaryAccessor.loadLimitOrders()
        for (order in orders) {
            addToOrderBook(order)
        }
        initialOrdersCount = orders.size
    }

    private fun addToOrderBook(order: LimitOrder) {
        val orderBook =
            limitOrdersQueues.getOrPut(order.assetPairId) { AssetOrderBook(order.brokerId, order.assetPairId) }
        orderBook.addOrder(order)
        addOrder(order)
    }

    fun addOrder(order: LimitOrder) {
        limitOrdersMap[order.externalId] = order
        clientLimitOrdersMap.getOrPut(order.clientId) { ArrayList() }.add(order)
        expiryOrdersQueue.addIfOrderHasExpiryTime(order)
    }

    override fun addOrders(orders: Collection<LimitOrder>) {
        orders.forEach { order ->
            addOrder(order)
        }
    }

    fun getAllOrderBooks() = limitOrdersQueues

    override fun getOrderBook(brokerId: String, assetPairId: String) =
        limitOrdersQueues[assetPairId] ?: AssetOrderBook(brokerId, assetPairId)

    override fun setOrderBook(assetPairId: String, assetOrderBook: AssetOrderBook) {
        limitOrdersQueues[assetPairId] = assetOrderBook
    }

    fun getOrder(uid: String) = limitOrdersMap[uid]

    fun searchOrders(clientId: String, assetPair: String?, isBuy: Boolean?): List<LimitOrder> {
        val result = mutableListOf<LimitOrder>()
        clientLimitOrdersMap[clientId]?.forEach { limitOrder ->
            if ((assetPair == null || limitOrder.assetPairId == assetPair) && (isBuy == null || limitOrder.isBuySide() == isBuy)) {
                result.add(limitOrder)
            }
        }
        return result
    }

    fun cancelLimitOrder(date: Date, uid: String, removeFromClientMap: Boolean = false): LimitOrder? {
        val order = limitOrdersMap.remove(uid) ?: return null
        expiryOrdersQueue.removeIfOrderHasExpiryTime(order)

        if (removeFromClientMap) {
            removeFromClientMap(uid)
        }

        getOrderBook(order.brokerId, order.assetPairId).removeOrder(order)
        order.updateStatus(Cancelled, date)
        return order
    }

    private fun removeFromClientMap(uid: String): Boolean {
        val order: LimitOrder = clientLimitOrdersMap.values.firstOrNull { it.any { it.externalId == uid } }
            ?.firstOrNull { it.externalId == uid } ?: return false
        return clientLimitOrdersMap[order.clientId]?.remove(order) ?: false
    }

    override fun cancelLimitOrders(orders: Collection<LimitOrder>, date: Date) {
        orders.forEach { order ->
            val ord = limitOrdersMap.remove(order.externalId)
            expiryOrdersQueue.removeIfOrderHasExpiryTime(order)
            clientLimitOrdersMap[order.clientId]?.remove(order)
            if (ord != null) {
                ord.updateStatus(Cancelled, date)
            }
        }
    }

    override fun removeOrdersFromMapsAndSetStatus(orders: Collection<LimitOrder>, status: OrderStatus?, date: Date?) {
        orders.forEach { order ->
            val removedOrder = limitOrdersMap.remove(order.externalId)
            clientLimitOrdersMap[order.clientId]?.remove(removedOrder)
            expiryOrdersQueue.removeIfOrderHasExpiryTime(order)
            if (removedOrder != null && status != null) {
                removedOrder.updateStatus(status, date!!)
            }
        }
    }

    fun buildMarketProfile(): List<BestPrice> {
        val result = LinkedList<BestPrice>()

        limitOrdersQueues.values.forEach { book ->
            val askPrice = book.getAskPrice()
            val bidPrice = book.getBidPrice()
            if (askPrice > BigDecimal.ZERO || bidPrice > BigDecimal.ZERO) {
                result.add(BestPrice(book.assetPairId, askPrice, bidPrice))
            }
        }

        return result
    }

    fun createCurrentTransactionOrderBooksHolder() = CurrentTransactionOrderBooksHolder(this)
}
