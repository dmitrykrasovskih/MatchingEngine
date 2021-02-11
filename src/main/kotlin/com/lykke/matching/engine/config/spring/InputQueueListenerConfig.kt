package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.incoming.listener.InputQueueListener
import com.lykke.matching.engine.incoming.preprocessor.impl.CashInOutPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.CashTransferPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.LimitOrderCancelOperationPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.LimitOrderMassCancelOperationPreprocessor
import com.lykke.matching.engine.incoming.preprocessor.impl.SingleLimitOrderPreprocessor
import com.lykke.matching.engine.messages.wrappers.*
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue

@Configuration
open class InputQueueListenerConfig {

    @Bean
    open fun cashTransferInputQueueListener(cashTransferInputQueue: BlockingQueue<CashTransferOperationMessageWrapper>,
                                            cashTransferPreprocessor: CashTransferPreprocessor,
                                            @Qualifier("cashTransferPreProcessingLogger")
                                            logger: ThrottlingLogger): InputQueueListener<CashTransferOperationMessageWrapper> {
        return InputQueueListener(cashTransferInputQueue,
                cashTransferPreprocessor,
                logger,
                "CashTransferInputQueueListener")
    }

    @Bean
    open fun cashInOutInputQueueListener(cashInOutInputQueue: BlockingQueue<CashInOutOperationMessageWrapper>,
                                         cashInOutPreprocessor: CashInOutPreprocessor,
                                         @Qualifier("cashInOutPreProcessingLogger")
                                         logger: ThrottlingLogger): InputQueueListener<CashInOutOperationMessageWrapper> {
        return InputQueueListener(cashInOutInputQueue,
                cashInOutPreprocessor,
                logger,
                "CashInOutInputQueueListener")
    }

    @Bean
    open fun limitOrderCancelInputQueueListener(limitOrderCancelInputQueue: BlockingQueue<LimitOrderCancelMessageWrapper>,
                                                limitOrderCancelOperationPreprocessor: LimitOrderCancelOperationPreprocessor,
                                                @Qualifier("limitOrderCancelPreProcessingLogger")
                                                logger: ThrottlingLogger): InputQueueListener<LimitOrderCancelMessageWrapper> {
        return InputQueueListener(limitOrderCancelInputQueue,
                limitOrderCancelOperationPreprocessor,
                logger,
                "LimitOrderCancelInputQueueListener")
    }

    @Bean
    open fun limitOrderInputQueueListener(limitOrderInputQueue: BlockingQueue<SingleLimitOrderMessageWrapper>,
                                          singleLimitOrderPreprocessor: SingleLimitOrderPreprocessor,
                                          @Qualifier("singleLimitOrderPreProcessingLogger")
                                          logger: ThrottlingLogger): InputQueueListener<SingleLimitOrderMessageWrapper> {
        return InputQueueListener(limitOrderInputQueue,
                singleLimitOrderPreprocessor,
                logger,
                "LimitOrderInputQueueListener")
    }

    @Bean
    open fun limitOrderMassCancelInputQueueListener(limitOrderMassCancelInputQueue: BlockingQueue<LimitOrderMassCancelMessageWrapper>,
                                                    limitOrderMassCancelOperationPreprocessor: LimitOrderMassCancelOperationPreprocessor,
                                                    @Qualifier("limitOrderMassCancelPreProcessingLogger")
                                                    logger: ThrottlingLogger): InputQueueListener<LimitOrderMassCancelMessageWrapper> {
        return InputQueueListener(limitOrderMassCancelInputQueue,
                limitOrderMassCancelOperationPreprocessor,
                logger,
                "LimitOrderMassCancelInputQueueListener")
    }

}