package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.google.protobuf.GeneratedMessageV3
import java.io.Serializable

interface EventPart<T : GeneratedMessageV3.Builder<T>> : Serializable {
    fun createGeneratedMessageBuilder(): T
}