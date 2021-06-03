package com.lykke.matching.engine.incoming.grpc

import com.lykke.matching.engine.messages.wrappers.CashInOutOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.CashSwapOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.CashTransferOperationMessageWrapper
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.myjetwallet.messages.incoming.grpc.CashServiceGrpc
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.BlockingQueue

class CashApiService(
    private val cashInOutInputQueue: BlockingQueue<CashInOutOperationMessageWrapper>,
    private val cashTransferInputQueue: BlockingQueue<CashTransferOperationMessageWrapper>,
    private val cashSwapInputQueue: BlockingQueue<CashSwapOperationMessageWrapper>,
    private val reservedCashInOutInputQueue: BlockingQueue<ReservedCashInOutOperationMessageWrapper>,
    registry: MeterRegistry
) : CashServiceGrpc.CashServiceImplBase() {

    private val cashInOutCounter = registry.counter("cash-inout-counter")
    private val cashTransferCounter = registry.counter("cash-transfer-counter")
    private val cashSwapCounter = registry.counter("cash-swap-counter")
    private val reservedCashInOutCounter = registry.counter("cash-reserved-inout-counter")

    override fun cashInOut(
        request: GrpcIncomingMessages.CashInOutOperation,
        responseObserver: StreamObserver<GrpcIncomingMessages.CashInOutOperationResponse>
    ) {
        cashInOutCounter.increment()
        cashInOutInputQueue.put(CashInOutOperationMessageWrapper(request, responseObserver, true))
    }

    override fun cashTransfer(
        request: GrpcIncomingMessages.CashTransferOperation,
        responseObserver: StreamObserver<GrpcIncomingMessages.CashTransferOperationResponse>
    ) {
        cashTransferCounter.increment()
        cashTransferInputQueue.put(CashTransferOperationMessageWrapper(request, responseObserver, true))
    }

    override fun cashSwap(
        request: GrpcIncomingMessages.CashSwapOperation,
        responseObserver: StreamObserver<GrpcIncomingMessages.CashSwapOperationResponse>
    ) {
        cashSwapCounter.increment()
        cashSwapInputQueue.put(CashSwapOperationMessageWrapper(request, responseObserver, true))
    }

    override fun reservedCashInOut(
        request: GrpcIncomingMessages.ReservedCashInOutOperation,
        responseObserver: StreamObserver<GrpcIncomingMessages.ReservedCashInOutOperationResponse>
    ) {
        reservedCashInOutCounter.increment()
        reservedCashInOutInputQueue.put(ReservedCashInOutOperationMessageWrapper(request, responseObserver, true))
    }
}