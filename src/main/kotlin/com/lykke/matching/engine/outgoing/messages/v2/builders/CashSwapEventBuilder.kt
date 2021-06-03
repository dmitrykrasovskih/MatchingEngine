package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.outgoing.messages.v2.enums.MessageType
import com.lykke.matching.engine.outgoing.messages.v2.events.CashSwapEvent
import com.lykke.matching.engine.outgoing.messages.v2.events.common.BalanceUpdate
import com.lykke.matching.engine.outgoing.messages.v2.events.common.CashSwap
import com.lykke.matching.engine.outgoing.messages.v2.events.common.Header

class CashSwapEventBuilder : EventBuilder<CashSwapData, CashSwapEvent>() {

    private var balanceUpdates: List<BalanceUpdate>? = null
    private var cashSwap: CashSwap? = null

    override fun getMessageType() = MessageType.CASH_SWAP

    override fun setEventData(eventData: CashSwapData): EventBuilder<CashSwapData, CashSwapEvent> {
        balanceUpdates = convertBalanceUpdates(eventData.clientBalanceUpdates)
        cashSwap = CashSwap(
            eventData.swapOperation.brokerId,
            eventData.swapOperation.accountId1,
            eventData.swapOperation.walletId1,
            eventData.swapOperation.asset1!!.symbol,
            bigDecimalToString(eventData.swapOperation.volume1),
            eventData.swapOperation.accountId2,
            eventData.swapOperation.walletId2,
            eventData.swapOperation.asset2!!.symbol,
            bigDecimalToString(eventData.swapOperation.volume2)
        )
        return this
    }

    override fun buildEvent(header: Header) = CashSwapEvent(header, balanceUpdates!!, cashSwap!!)

}