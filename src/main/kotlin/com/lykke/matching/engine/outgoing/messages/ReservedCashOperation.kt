package com.lykke.matching.engine.outgoing.messages

import java.util.*

class ReservedCashOperation(
    val id: String,
    val clientId: String,
    val dateTime: Date,
    val reservedVolume: String,
    val reservedForSwapVolume: String,
    var asset: String,
    val messageId: String
)