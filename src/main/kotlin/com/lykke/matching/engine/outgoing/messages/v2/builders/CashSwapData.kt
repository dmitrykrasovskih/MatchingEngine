package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate

class CashSwapData(
    val clientBalanceUpdates: List<ClientBalanceUpdate>,
    val swapOperation: SwapOperation
) : EventData