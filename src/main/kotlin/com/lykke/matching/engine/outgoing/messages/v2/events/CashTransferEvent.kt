package com.lykke.matching.engine.outgoing.messages.v2.events

import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashTransfer
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class CashTransferEvent(
    header: Header,
    val balanceUpdates: List<BalanceUpdate>,
    val cashTransfer: CashTransfer
) : Event<OutgoingMessages.OutgoingEvent>(header) {

    override fun buildGeneratedMessage(): OutgoingMessages.OutgoingEvent {
        val builder = OutgoingMessages.OutgoingEvent.newBuilder()
        builder.setHeader(header.createGeneratedMessageBuilder())
        balanceUpdates.forEach { balanceUpdate ->
            builder.addBalanceUpdates(balanceUpdate.createGeneratedMessageBuilder())
        }
        builder.setCashTransfer(cashTransfer.createGeneratedMessageBuilder())
        return builder.build()
    }

}