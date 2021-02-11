package com.lykke.matching.engine.order

import com.lykke.matching.engine.AbstractTest
import com.lykke.matching.engine.config.TestApplicationContext
import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.daos.order.OrderTimeInForce
import com.lykke.matching.engine.messages.wrappers.LimitOrderCancelMessageWrapper
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.utils.MessageBuilder.Companion.buildLimitOrder
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import java.util.concurrent.BlockingQueue
import kotlin.test.assertEquals

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [(TestApplicationContext::class)])
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ExpiredOrdersCancellerTest : AbstractTest() {

    @Autowired
    private lateinit var preProcessedMessageQueue: BlockingQueue<MessageWrapper>

    @Autowired
    private lateinit var expiryOrdersQueue: ExpiryOrdersQueue

    private val orders = mutableListOf<LimitOrder>()

    @Before
    fun setUp() {
        val now = Date()
        orders.addAll(
            listOf(
                buildLimitOrder(timeInForce = OrderTimeInForce.GTD, expiryTime = date(now, 500), uid = "1"),
                buildLimitOrder(timeInForce = OrderTimeInForce.GTD, expiryTime = date(now, 1500), uid = "2")
            )
        )
        orders.forEach { expiryOrdersQueue.addIfOrderHasExpiryTime(it) }
    }

    @Test
    fun testCancelExpiredOrders() {
        val service = ExpiredOrdersCanceller(expiryOrdersQueue, preProcessedMessageQueue)

        service.cancelExpiredOrders()
        assertEquals(0, preProcessedMessageQueue.size)

        Thread.sleep(800)
        service.cancelExpiredOrders()
        assertEquals(1, preProcessedMessageQueue.size)
        var messageContext =
            (preProcessedMessageQueue.poll() as LimitOrderCancelMessageWrapper).context as LimitOrderCancelOperationContext
        assertEquals(1, messageContext.limitOrderIds.size)
        assertEquals("1", messageContext.limitOrderIds.single())

        preProcessedMessageQueue.clear()
        expiryOrdersQueue.removeIfOrderHasExpiryTime(orders[0])

        Thread.sleep(800)
        service.cancelExpiredOrders()
        assertEquals(1, preProcessedMessageQueue.size)
        messageContext =
            (preProcessedMessageQueue.poll() as LimitOrderCancelMessageWrapper).context as LimitOrderCancelOperationContext
        assertEquals(1, messageContext.limitOrderIds.size)
        assertEquals("2", messageContext.limitOrderIds.single())
    }

    private fun date(date: Date, delta: Long) = Date(date.time + delta)
}