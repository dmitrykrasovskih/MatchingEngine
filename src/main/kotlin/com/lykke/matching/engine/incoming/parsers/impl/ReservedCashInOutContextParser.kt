package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.ReservedCashInOutOperation
import com.lykke.matching.engine.daos.context.ReservedCashInOutContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.ReservedCashInOutParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.lykke.matching.engine.utils.proto.toDate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class ReservedCashInOutContextParser(private val assetsHolder: AssetsHolder) :
    ContextParser<ReservedCashInOutParsedData, ReservedCashInOutOperationMessageWrapper> {
    override fun parse(messageWrapper: ReservedCashInOutOperationMessageWrapper): ReservedCashInOutParsedData {
        val operationId = UUID.randomUUID().toString()

        val message = messageWrapper.parsedMessage

        messageWrapper.processedMessage = ProcessedMessage(
            MessageType.CASH_IN_OUT_OPERATION.type,
            message.timestamp.seconds,
            messageWrapper.messageId
        )

        messageWrapper.context = ReservedCashInOutContext(
            messageWrapper.messageId,
            messageWrapper.processedMessage!!,
            ReservedCashInOutOperation(
                operationId,
                message.id,
                message.brokerId,
                message.accountId,
                message.walletId,
                assetsHolder.getAssetAllowNulls(message.assetId),
                message.timestamp.toDate(),
                if (!message.reservedVolume.isNullOrEmpty()) BigDecimal(message.reservedVolume) else BigDecimal.ZERO,
                if (!message.reservedForSwapVolume.isNullOrEmpty()) BigDecimal(message.reservedForSwapVolume) else BigDecimal.ZERO
            )
        )

        return ReservedCashInOutParsedData(messageWrapper, message.assetId)
    }
}