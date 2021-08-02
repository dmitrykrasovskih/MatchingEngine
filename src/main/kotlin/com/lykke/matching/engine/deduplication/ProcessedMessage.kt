package com.lykke.matching.engine.deduplication

import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.order.process.ProcessedOrder
import org.nustaq.serialization.annotations.Version
import java.io.Serializable
import java.math.BigDecimal

data class ProcessedMessage(
    val type: Byte,
    val timestamp: Long,
    val messageId: String,
    @Version(2)
    var status: MessageStatus? = null,
    var statusReason: String? = null,
    //cash operations
    var matchingEngineId: String? = null,
    //orders
    var orderId: String? = null,
    var walletVersion: Long? = null,
    //multi limit orders
    var processedOrders: List<ProcessedOrder>? = null,
    //market orders
    var price: BigDecimal? = null,
    var isStraight: Boolean? = null,
    var volume: BigDecimal? = null,
    var oppositeVolume: BigDecimal? = null

) : Serializable {
    override fun toString(): String {
        return "ProcessedMessage(type=$type, timestamp=$timestamp, messageId='$messageId')"
    }
}