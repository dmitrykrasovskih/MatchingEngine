package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.CashInOutOperation
import com.lykke.matching.engine.daos.context.CashInOutContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashInOutParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper
import com.lykke.matching.engine.utils.proto.toDate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashInOutContextParser(private val assetsHolder: AssetsHolder) :
    ContextParser<CashInOutParsedData, CashInOutOperationMessageWrapper> {

    override fun parse(messageWrapper: CashInOutOperationMessageWrapper): CashInOutParsedData {
        val operationId = UUID.randomUUID().toString()

        val message = messageWrapper.parsedMessage

        messageWrapper.processedMessage = ProcessedMessage(
            MessageType.CASH_IN_OUT_OPERATION.type,
            message.timestamp.seconds,
            messageWrapper.messageId
        )

        messageWrapper.context = CashInOutContext(
            messageWrapper.messageId,
            messageWrapper.processedMessage!!,
            CashInOutOperation(
                operationId,
                message.id,
                message.brokerId,
                message.accountId,
                message.walletId,
                assetsHolder.getAssetAllowNulls(message.assetId),
                message.timestamp.toDate(),
                BigDecimal(message.volume),
                feeInstructions = NewFeeInstruction.create(message.feesList)
            )
        )

        return CashInOutParsedData(messageWrapper, message.assetId)
    }
}