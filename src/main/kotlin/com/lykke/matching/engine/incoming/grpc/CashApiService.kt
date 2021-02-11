package com.lykke.matching.engine.incoming.grpc

import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.myjetwallet.messages.incoming.grpc.CashServiceGrpc
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import java.util.concurrent.BlockingQueue

class CashApiService(
        private val cashInOutInputQueue: BlockingQueue<CashInOutOperationMessageWrapper>,
        private val cashTransferInputQueue: BlockingQueue<CashTransferOperationMessageWrapper>) : CashServiceGrpc.CashServiceImplBase() {
    override fun cashInOut(request: GrpcIncomingMessages.CashInOutOperation, responseObserver: StreamObserver<GrpcIncomingMessages.CashInOutOperationResponse>) {
        cashInOutInputQueue.put(CashInOutOperationMessageWrapper(request, responseObserver, true))
    }

    override fun cashTransfer(request: GrpcIncomingMessages.CashTransferOperation, responseObserver: StreamObserver<GrpcIncomingMessages.CashTransferOperationResponse>) {
        cashTransferInputQueue.put(CashTransferOperationMessageWrapper(request, responseObserver, true))
    }

    override fun reservedCashInOut(request: GrpcIncomingMessages.ReservedCashInOutOperation, responseObserver: StreamObserver<GrpcIncomingMessages.ReservedCashInOutOperationResponse>) {
        //not implemented
    }
}