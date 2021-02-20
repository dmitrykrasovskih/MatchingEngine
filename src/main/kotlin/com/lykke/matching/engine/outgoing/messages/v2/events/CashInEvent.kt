package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashIn
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class CashInEvent(
    header: Header,
    val balanceUpdates: List<BalanceUpdate>,
    val cashIn: CashIn
) : Event<OutgoingMessages.OutgoingEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.OutgoingEvent {
        val builder = OutgoingMessages.OutgoingEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashIn(cashIn.createGeneratedMessageBuilder())
        return builder.build()
    }

}