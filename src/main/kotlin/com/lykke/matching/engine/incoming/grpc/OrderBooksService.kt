package com.lykke.matching.engine.incoming.grpc

import com.google.protobuf.Empty
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.outgoing.messages.OrderBook
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.NumberUtils
import com.lykke.matching.engine.utils.proto.createProtobufTimestampBuilder
import com.myjetwallet.messages.incoming.grpc.OrderBooksServiceGrpc
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages
import io.grpc.stub.StreamObserver
import java.util.*

class OrderBooksService(
    private val genericLimitOrderService: GenericLimitOrderService,
    private val assetsCache: AssetsHolder,
    private val assetPairsCache: AssetsPairsHolder
) : OrderBooksServiceGrpc.OrderBooksServiceImplBase() {

    override fun orderBookSnapshots(
        request: Empty,
        responseObserver: StreamObserver<OutgoingMessages.OrderBookSnapshot>
    ) {
        val now = Date()
        val orderBooks = genericLimitOrderService.getAllOrderBooks()
        orderBooks.values.forEach {
            val orderBook = it.copy()
            responseObserver.onNext(
                buildOrderBook(
                    OrderBook(
                        orderBook.brokerId,
                        orderBook.assetPairId,
                        true,
                        now,
                        orderBook.getOrderBook(true)
                    )
                )
            )
            responseObserver.onNext(
                buildOrderBook(
                    OrderBook(
                        orderBook.brokerId,
                        orderBook.assetPairId,
                        false,
                        now,
                        orderBook.getOrderBook(false)
                    )
                )
            )
        }
        responseObserver.onCompleted()
    }

    private fun buildOrderBook(orderBook: OrderBook): OutgoingMessages.OrderBookSnapshot {
        val builder = OutgoingMessages.OrderBookSnapshot.newBuilder()
            .setBrokerId(orderBook.brokerId)
            .setAsset(orderBook.assetPair).setIsBuy(orderBook.isBuy)
            .setTimestamp(orderBook.timestamp.createProtobufTimestampBuilder())
        val pair = assetPairsCache.getAssetPair(orderBook.brokerId, orderBook.assetPair)
        val baseAsset = assetsCache.getAsset(pair.baseAssetId)
        orderBook.prices.forEach { orderBookPrice ->
            builder.addLevels(
                OutgoingMessages.OrderBookSnapshot.OrderBookLevel.newBuilder()
                    .setPrice(NumberUtils.setScaleRoundHalfUp(orderBookPrice.price, pair.accuracy).toPlainString())
                    .setVolume(
                        NumberUtils.setScaleRoundHalfUp(orderBookPrice.volume, baseAsset.accuracy).toPlainString()
                    )
                    .setWalletId(orderBookPrice.clientId)
                    .setOrderId(orderBookPrice.id).build()
            )
        }
        return builder.build()
    }
}