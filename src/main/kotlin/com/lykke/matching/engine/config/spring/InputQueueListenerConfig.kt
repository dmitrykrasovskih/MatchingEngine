package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.incoming.listener.InputQueueListener
import com.lykke.matching.engine.incoming.preprocessor.impl.*
import com.lykke.matching.engine.messages.wrappers.*
import com.lykke.matching.engine.messages.wrappers.socket.LimitOrderMassCancelMessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.BlockingQueue

@Configuration
class InputQueueListenerConfig {

    @Bean
    fun cashTransferInputQueueListener(
        cashTransferInputQueue: BlockingQueue<CashTransferOperationMessageWrapper>,
        cashTransferPreprocessor: CashTransferPreprocessor,
        @Qualifier("cashTransferPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<CashTransferOperationMessageWrapper> {
        return InputQueueListener(
            cashTransferInputQueue,
            cashTransferPreprocessor,
            logger,
            "CashTransferInputQueueListener"
        )
    }

    @Bean
    fun cashSwapInputQueueListener(
        cashSwapInputQueue: BlockingQueue<CashSwapOperationMessageWrapper>,
        cashSwapPreprocessor: CashSwapPreprocessor,
        @Qualifier("cashTransferPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<CashSwapOperationMessageWrapper> {
        return InputQueueListener(
            cashSwapInputQueue,
            cashSwapPreprocessor,
            logger,
            "CashTransferInputQueueListener"
        )
    }

    @Bean
    fun cashInOutInputQueueListener(
        cashInOutInputQueue: BlockingQueue<CashInOutOperationMessageWrapper>,
        cashInOutPreprocessor: CashInOutPreprocessor,
        @Qualifier("cashInOutPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<CashInOutOperationMessageWrapper> {
        return InputQueueListener(
            cashInOutInputQueue,
            cashInOutPreprocessor,
            logger,
            "CashInOutInputQueueListener"
        )
    }

    @Bean
    fun reservedCashInOutInputQueueListener(
        reservedCashInOutInputQueue: BlockingQueue<ReservedCashInOutOperationMessageWrapper>,
        reservedCashInOutPreprocessor: ReservedCashInOutPreprocessor,
        @Qualifier("cashInOutPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<ReservedCashInOutOperationMessageWrapper> {
        return InputQueueListener(
            reservedCashInOutInputQueue,
            reservedCashInOutPreprocessor,
            logger,
            "CashInOutInputQueueListener"
        )
    }

    @Bean
    fun limitOrderCancelInputQueueListener(
        limitOrderCancelInputQueue: BlockingQueue<LimitOrderCancelMessageWrapper>,
        limitOrderCancelOperationPreprocessor: LimitOrderCancelOperationPreprocessor,
        @Qualifier("limitOrderCancelPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<LimitOrderCancelMessageWrapper> {
        return InputQueueListener(
            limitOrderCancelInputQueue,
            limitOrderCancelOperationPreprocessor,
            logger,
            "LimitOrderCancelInputQueueListener"
        )
    }

    @Bean
    fun limitOrderInputQueueListener(
        limitOrderInputQueue: BlockingQueue<SingleLimitOrderMessageWrapper>,
        singleLimitOrderPreprocessor: SingleLimitOrderPreprocessor,
        @Qualifier("singleLimitOrderPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<SingleLimitOrderMessageWrapper> {
        return InputQueueListener(
            limitOrderInputQueue,
            singleLimitOrderPreprocessor,
            logger,
            "LimitOrderInputQueueListener"
        )
    }

    @Bean
    fun limitOrderMassCancelInputQueueListener(
        limitOrderMassCancelInputQueue: BlockingQueue<LimitOrderMassCancelMessageWrapper>,
        limitOrderMassCancelOperationPreprocessor: LimitOrderMassCancelOperationPreprocessor,
        @Qualifier("limitOrderMassCancelPreProcessingLogger")
        logger: ThrottlingLogger
    ): InputQueueListener<LimitOrderMassCancelMessageWrapper> {
        return InputQueueListener(
            limitOrderMassCancelInputQueue,
            limitOrderMassCancelOperationPreprocessor,
            logger,
            "LimitOrderMassCancelInputQueueListener"
        )
    }

}