package com.lykke.matching.engine.incoming

//@Component
//class MessageRouter(
////        private val limitOrderInputQueue: BlockingQueue<MessageWrapper>,
////        private val cashInOutInputQueue: BlockingQueue<CashInOutOperationMessageWrapper>,
////        private val cashTransferInputQueue: BlockingQueue<CashTransferOperationMessageWrapper>
////        private val limitOrderCancelInputQueue: BlockingQueue<MessageWrapper>,
////        private val limitOrderMassCancelInputQueue: BlockingQueue<MessageWrapper>,
////        val preProcessedMessageQueue: BlockingQueue<MessageWrapper>
//) {
//    fun process() {
////        when(wrapper.type) {
////            MessageType.CASH_IN_OUT_OPERATION -> cashInOutInputQueue.put(wrapper as CashInOutOperationMessageWrapper)
////            MessageType.CASH_TRANSFER_OPERATION -> cashTransferInputQueue.put(wrapper as CashTransferOperationMessageWrapper)
//////            MessageType.LIMIT_ORDER -> limitOrderInputQueue.put(wrapper)
//////            MessageType.LIMIT_ORDER_CANCEL -> limitOrderCancelInputQueue.put(wrapper)
//////            MessageType.LIMIT_ORDER_MASS_CANCEL -> limitOrderMassCancelInputQueue.put(wrapper)
////
//////            else -> preProcessedMessageQueue.put(wrapper)
////            else -> println("unknown type")
////        }
//    }
//}