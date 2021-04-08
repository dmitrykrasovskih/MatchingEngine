package com.lykke.matching.engine.outgoing.messages.v2.builders

import com.lykke.matching.engine.daos.WalletOperation
import com.lykke.matching.engine.outgoing.messages.ClientBalanceUpdate

class ReservedCashInOutEventData(
    val clientBalanceUpdates: List<ClientBalanceUpdate>,
    val reservedCashInOutOperation: WalletOperation
) : EventData