package com.lykke.matching.engine.incoming.preprocessor.impl

import com.lykke.matching.engine.deduplication.ProcessedMessage
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.data.LimitOrderMassCancelOperationParsedData
import com.lykke.matching.engine.incoming.parsers.impl.LimitOrderMassCancelOperationContextParser
import com.lykke.matching.engine.incoming.preprocessor.AbstractMessagePreprocessor
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class LimitOrderMassCancelOperationPreprocessor(
    limitOrderMassCancelOperationContextParser: LimitOrderMassCancelOperationContextParser,
    messageProcessingStatusHolder: MessageProcessingStatusHolder,
    preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    @Qualifier("limitOrderMassCancelPreProcessingLogger")
    private val logger: ThrottlingLogger
) :
    AbstractMessagePreprocessor<LimitOrderMassCancelOperationParsedData, LimitOrderMassCancelMessageWrapper>(
        limitOrderMassCancelOperationContextParser,
        messageProcessingStatusHolder,
        preProcessedMessageQueue,
        logger
    ) {

    override fun preProcessParsedData(parsedData: LimitOrderMassCancelOperationParsedData): Boolean {
        return true
    }

    override fun writeResponse(
        messageWrapper: LimitOrderMassCancelMessageWrapper,
        status: MessageStatus,
        message: String?
    ) {
        messageWrapper.writeResponse(status, message)
    }

    override fun writeResponse(messageWrapper: LimitOrderMassCancelMessageWrapper, processedMessage: ProcessedMessage) {
        //do nothing
    }
}
