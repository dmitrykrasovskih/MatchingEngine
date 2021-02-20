package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashOut
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class CashOutEvent(
    header: Header,
    val balanceUpdates: List<BalanceUpdate>,
    val cashOut: CashOut
) : Event<OutgoingMessages.OutgoingEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.OutgoingEvent {
        val builder = OutgoingMessages.OutgoingEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashOut(cashOut.createGeneratedMessageBuilder())
        return builder.build()
    }

}

