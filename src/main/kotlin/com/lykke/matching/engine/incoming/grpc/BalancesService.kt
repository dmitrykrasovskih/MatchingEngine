package com.lykke.matching.engine.incoming.grpc

import com.lykke.matching.engine.daos.wallet.AssetBalance
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.utils.proto.createProtobufTimestampBuilder
import com.matching.engine.database.grpc.BalancesMessages
import com.myjetwallet.messages.incoming.grpc.BalancesServiceGrpc
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.MeterRegistry
import java.util.*

class BalancesService(
    private val balancesHolder: BalancesHolder,
    registry: MeterRegistry
) : BalancesServiceGrpc.BalancesServiceImplBase() {

    private val balancesGetAllCounter = registry.counter("balances-getall-counter")
    private val balancesGetByAssetCounter = registry.counter("balances-getbyasset-counter")

    override fun getAll(
        request: BalancesMessages.BalancesGetAllRequest,
        responseObserver: StreamObserver<BalancesMessages.BalancesGetAllResponse>
    ) {
        balancesGetAllCounter.increment()
        val balances = balancesHolder.getBalances(request.walletId)
        responseObserver.onNext(buildBalanceAllResponse(Date(), request.walletId, balances.values))
        responseObserver.onCompleted()
    }

    override fun getByAssetId(
        request: BalancesMessages.BalancesGetByAssetIdRequest,
        responseObserver: StreamObserver<BalancesMessages.BalancesGetByAssetIdResponse>
    ) {
        balancesGetByAssetCounter.increment()
        val balances = balancesHolder.getBalances(request.walletId)
        responseObserver.onNext(buildBalanceByIdResponse(Date(), request.walletId, balances[request.assetId]))
        responseObserver.onCompleted()
    }

    private fun buildBalanceAllResponse(
        now: Date,
        walletId: String,
        filteredBalances: Collection<AssetBalance>
    ): BalancesMessages.BalancesGetAllResponse {
        val builder = BalancesMessages.BalancesGetAllResponse.newBuilder()
        builder.walletId = walletId
        builder.timestamp = now.createProtobufTimestampBuilder().build()
        filteredBalances.forEach {
            builder.addBalances(
                BalancesMessages.Balance.newBuilder()
                    .setAssetId(it.asset)
                    .setAmount(it.balance.toPlainString())
                    .setReserved(it.getTotalReserved().toPlainString())
                    .setReservedForSwapVolume(it.reservedForSwap.toPlainString())
                    .setReservedForOrders(it.reserved.toPlainString())
            )
        }
        return builder.build()
    }

    private fun buildBalanceByIdResponse(
        now: Date,
        walletId: String,
        balance: AssetBalance?
    ): BalancesMessages.BalancesGetByAssetIdResponse {
        val builder = BalancesMessages.BalancesGetByAssetIdResponse.newBuilder()
        builder.walletId = walletId
        builder.timestamp = now.createProtobufTimestampBuilder().build()
        if (balance != null) {
            builder.balance = BalancesMessages.Balance.newBuilder()
                .setAssetId(balance.asset)
                .setAmount(balance.balance.toPlainString())
                .setReserved(balance.getTotalReserved().toPlainString())
                .setReservedForSwapVolume(balance.reservedForSwap.toPlainString())
                .setReservedForOrders(balance.reserved.toPlainString())
                .build()
        }
        return builder.build()
    }
}