package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.SwapOperation
import com.lykke.matching.engine.daos.context.CashSwapContext
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashSwapParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.CashSwapOperationMessageWrapper
import com.lykke.matching.engine.utils.proto.toDate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashSwapContextParser(private val assetsHolder: AssetsHolder) :
    ContextParser<CashSwapParsedData, CashSwapOperationMessageWrapper> {
    override fun parse(messageWrapper: CashSwapOperationMessageWrapper): CashSwapParsedData {
        val message = messageWrapper.parsedMessage

        val swapOperation = SwapOperation(
            UUID.randomUUID().toString(),
            message.id,
            message.brokerId,
            message.accountId1,
            message.walletId1,
            assetsHolder.getAssetAllowNulls(message.assetId1),
            BigDecimal(message.volume1),
            message.accountId2,
            message.walletId2,
            assetsHolder.getAssetAllowNulls(message.assetId2),
            BigDecimal(message.volume2),
            message.timestamp.toDate()
        )

        messageWrapper.processedMessage = ProcessedMessage(
            MessageType.CASH_SWAP_OPERATION.type,
            swapOperation.dateTime.time,
            swapOperation.externalId
        )

        messageWrapper.context =
            CashSwapContext(
                if (message.hasMessageId()) message.messageId.value else message.id,
                swapOperation,
                messageWrapper.processedMessage!!
            )

        return CashSwapParsedData(messageWrapper, message.assetId1, message.assetId2)
    }
}