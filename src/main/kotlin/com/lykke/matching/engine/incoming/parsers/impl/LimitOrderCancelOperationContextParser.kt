package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.context.LimitOrderCancelOperationContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderCancelOperationParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.LimitOrderCancelMessageWrapper
import org.springframework.stereotype.Component
import java.util.*

@Component
class LimitOrderCancelOperationContextParser :
    ContextParser<LimitOrderCancelOperationParsedData, LimitOrderCancelMessageWrapper> {
    override fun parse(messageWrapper: LimitOrderCancelMessageWrapper): LimitOrderCancelOperationParsedData {
        messageWrapper.context = parseContext(messageWrapper)
        return LimitOrderCancelOperationParsedData(messageWrapper)
    }

    private fun parseContext(messageWrapper: LimitOrderCancelMessageWrapper): LimitOrderCancelOperationContext {
        val message = messageWrapper.parsedMessage
        messageWrapper.processedMessage =
            ProcessedMessage(messageWrapper.type.type, Date().time, messageWrapper.messageId)

        return LimitOrderCancelOperationContext(
            message.id,
            messageWrapper.messageId,
            messageWrapper.processedMessage!!,
            message.limitOrderIdList.toSet(), getMessageType(messageWrapper.type.type)
        )
    }

    private fun getMessageType(type: Byte): MessageType {
        return MessageType.valueOf(type) ?: throw Exception("Unknown message type $type")
    }
}