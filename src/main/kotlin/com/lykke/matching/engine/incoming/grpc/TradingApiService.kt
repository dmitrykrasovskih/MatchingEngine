package com.lykke.matching.engine.incoming.grpc

import com.lykke.matching.engine.messages.wrappers.*
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import com.myjetwallet.messages.incoming.grpc.TradingServiceGrpc
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.BlockingQueue

class TradingApiService(
    private val limitOrderInputQueue: BlockingQueue<SingleLimitOrderMessageWrapper>,
    private val limitOrderCancelInputQueue: BlockingQueue<LimitOrderCancelMessageWrapper>,
    private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
    registry: MeterRegistry
) : TradingServiceGrpc.TradingServiceImplBase() {

    private val marketOrderCounter = registry.counter("orders-market-counter")
    private val limitOrderCounter = registry.counter("orders-limit-counter")
    private val cancelOrderCounter = registry.counter("orders-cancel-counter")
    private val multiOrderCounter = registry.counter("orders-multi-counter")

    private val marketOrderTimer = registry.timer("order-market-timer")
    private val limitOrderTimer = registry.timer("order-limit-timer")
    private val cancelOrderTimer = registry.timer("order-cancel-timer")
    private val multiOrderTimer = registry.timer("order-multi-timer")

    override fun marketOrder(
        request: GrpcIncomingMessages.MarketOrder,
        responseObserver: StreamObserver<GrpcIncomingMessages.MarketOrderResponse>
    ) {
        marketOrderCounter.increment()
        preProcessedMessageQueue.put(MarketOrderMessageWrapper(request, responseObserver, marketOrderTimer, true))
    }

    override fun limitOrder(
        request: GrpcIncomingMessages.LimitOrder,
        responseObserver: StreamObserver<GrpcIncomingMessages.LimitOrderResponse>
    ) {
        limitOrderCounter.increment()
        limitOrderInputQueue.put(SingleLimitOrderMessageWrapper(request, responseObserver, limitOrderTimer, true))
    }

    override fun cancelLimitOrder(
        request: GrpcIncomingMessages.LimitOrderCancel,
        responseObserver: StreamObserver<GrpcIncomingMessages.LimitOrderCancelResponse>
    ) {
        cancelOrderCounter.increment()
        limitOrderCancelInputQueue.put(
            LimitOrderCancelMessageWrapper(
                request,
                responseObserver,
                cancelOrderTimer,
                true
            )
        )
    }

    override fun multiLimitOrder(
        request: GrpcIncomingMessages.MultiLimitOrder,
        responseObserver: StreamObserver<GrpcIncomingMessages.MultiLimitOrderResponse>
    ) {
        multiOrderCounter.increment()
        preProcessedMessageQueue.put(MultiLimitOrderMessageWrapper(request, responseObserver, multiOrderTimer, true))
    }
}