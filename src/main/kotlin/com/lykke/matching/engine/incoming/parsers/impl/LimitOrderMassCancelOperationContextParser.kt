package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.context.LimitOrderMassCancelOperationContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import org.springframework.stereotype.Component
import java.util.*

@Component
class LimitOrderMassCancelOperationContextParser :
    ContextParser<LimitOrderMassCancelOperationParsedData, LimitOrderMassCancelMessageWrapper> {
    override fun parse(messageWrapper: LimitOrderMassCancelMessageWrapper): LimitOrderMassCancelOperationParsedData {
        messageWrapper.context = parseMessage(messageWrapper)
        return LimitOrderMassCancelOperationParsedData(messageWrapper)
    }

    private fun parseMessage(messageWrapper: LimitOrderMassCancelMessageWrapper): LimitOrderMassCancelOperationContext {
        val message = messageWrapper.parsedMessage

        messageWrapper.processedMessage =
            ProcessedMessage(messageWrapper.type.type, Date().time, messageWrapper.messageId)

        val messageType = MessageType.valueOf(messageWrapper.type.type)
            ?: throw Exception("Unknown message type ${messageWrapper.type}")

        val assetPairId = if (message.hasAssetPairId()) message.assetPairId.value else null
        val isBuy = if (message.hasIsBuy()) message.isBuy.value else null

        return LimitOrderMassCancelOperationContext(
            message.id, messageWrapper.messageId, message.walletId.value,
            messageWrapper.processedMessage!!, messageType,
            assetPairId, isBuy
        )
    }
}