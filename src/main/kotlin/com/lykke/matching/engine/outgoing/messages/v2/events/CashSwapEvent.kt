package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashSwap
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class CashSwapEvent(
    header: Header,
    val balanceUpdates: List<BalanceUpdate>,
    val cashSwap: CashSwap
) : Event<OutgoingMessages.OutgoingEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.OutgoingEvent {
        val builder = OutgoingMessages.OutgoingEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashSwap(cashSwap.createGeneratedMessageBuilder())
        return builder.build()
    }

}