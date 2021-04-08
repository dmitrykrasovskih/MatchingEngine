package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.events.ReservedCashInOutEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header
import com.lykke.matching.engine.outgoing.messages.v2.events.common.ReservedCashInOut

class ReservedCashInOutEventBuilder : EventBuilder<ReservedCashInOutEventData, ReservedCashInOutEvent>() {

    private var balanceUpdates: List<BalanceUpdate>? = null
    private var reservedCashInOut: ReservedCashInOut? = null

    override fun getMessageType() = MessageType.CASH_IN

    override fun setEventData(eventData: ReservedCashInOutEventData): EventBuilder<ReservedCashInOutEventData, ReservedCashInOutEvent> {
        balanceUpdates = convertBalanceUpdates(eventData.clientBalanceUpdates)
        reservedCashInOut = ReservedCashInOut(
            eventData.reservedCashInOutOperation.brokerId,
            eventData.reservedCashInOutOperation.accountId,
            eventData.reservedCashInOutOperation.clientId,
            eventData.reservedCashInOutOperation.assetId,
            bigDecimalToString(eventData.reservedCashInOutOperation.reservedAmount),
            bigDecimalToString(eventData.reservedCashInOutOperation.reservedForSwapAmount)
        )
        return this
    }

    override fun buildEvent(header: Header) = ReservedCashInOutEvent(header, balanceUpdates!!, reservedCashInOut!!)

}