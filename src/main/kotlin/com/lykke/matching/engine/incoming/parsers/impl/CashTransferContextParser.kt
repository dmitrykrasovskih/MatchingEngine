package com.lykke.matching.engine.incoming.parsers.impl

import com.lykke.matching.engine.daos.TransferOperation
import com.lykke.matching.engine.daos.context.CashTransferContext
import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.CashTransferParsedData
import com.lykke.matching.engine.messages.MessageType
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.utils.proto.toDate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.*

@Component
class CashTransferContextParser(private val assetsHolder: AssetsHolder) :
    ContextParser<CashTransferParsedData, CashTransferOperationMessageWrapper> {
    override fun parse(messageWrapper: CashTransferOperationMessageWrapper): CashTransferParsedData {
        val message = messageWrapper.parsedMessage

        val feeInstructions = NewFeeInstruction.create(message.feesList)

        val transferOperation = TransferOperation(
            UUID.randomUUID().toString(),
            message.id,
            message.brokerId,
            message.accountId,
            message.fromWalletId,
            message.toWalletId,
            assetsHolder.getAssetAllowNulls(message.assetId),
            message.timestamp.toDate(),
            BigDecimal(message.volume),
            if (message.hasOverdraftLimit()) BigDecimal(message.overdraftLimit.value) else null,
            feeInstructions
        )

        messageWrapper.processedMessage = ProcessedMessage(
            MessageType.CASH_TRANSFER_OPERATION.type,
            transferOperation.dateTime.time,
            transferOperation.externalId
        )

        messageWrapper.context =
            CashTransferContext(
                if (message.hasMessageId()) message.messageId.value else message.id,
                transferOperation,
                messageWrapper.processedMessage!!
            )

        return CashTransferParsedData(messageWrapper, message.assetId, feeInstructions)
    }
}