package com.lykke.matching.engine.incoming.grpc

import com.lykke.matching.engine.messages.wrappers.*
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import com.myjetwallet.messages.incoming.grpc.TradingServiceGrpc
import io.grpc.stub.StreamObserver
import java.util.concurrent.BlockingQueue

class TradingApiService(
    private val limitOrderInputQueue: BlockingQueue<SingleLimitOrderMessageWrapper>,
    private val limitOrderCancelInputQueue: BlockingQueue<LimitOrderCancelMessageWrapper>,
    private val preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
) : TradingServiceGrpc.TradingServiceImplBase() {

    override fun marketOrder(
        request: GrpcIncomingMessages.MarketOrder,
        responseObserver: StreamObserver<GrpcIncomingMessages.MarketOrderResponse>
    ) {
        preProcessedMessageQueue.put(MarketOrderMessageWrapper(request, responseObserver, true))
    }

    override fun limitOrder(
        request: GrpcIncomingMessages.LimitOrder,
        responseObserver: StreamObserver<GrpcIncomingMessages.LimitOrderResponse>
    ) {
        limitOrderInputQueue.put(SingleLimitOrderMessageWrapper(request,  responseObserver, true))
    }

    override fun cancelLimitOrder(
        request: GrpcIncomingMessages.LimitOrderCancel,
        responseObserver: StreamObserver<GrpcIncomingMessages.LimitOrderCancelResponse>
    ) {
        limitOrderCancelInputQueue.put(LimitOrderCancelMessageWrapper(request, responseObserver, true))
    }

    override fun multiLimitOrder(
        request: GrpcIncomingMessages.MultiLimitOrder,
        responseObserver: StreamObserver<GrpcIncomingMessages.MultiLimitOrderResponse>
    ) {
        preProcessedMessageQueue.put(MultiLimitOrderMessageWrapper(request, responseObserver, true))
    }
}