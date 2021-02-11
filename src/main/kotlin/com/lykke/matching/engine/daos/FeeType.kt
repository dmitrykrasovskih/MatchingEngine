package com.lykke.matching.engine.daos

import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages


enum class FeeType constructor(val externalId: GrpcIncomingMessages.FeeType) {

    NO_FEE(GrpcIncomingMessages.FeeType.NO_FEE),
    CLIENT_FEE(GrpcIncomingMessages.FeeType.CLIENT_FEE),
    EXTERNAL_FEE(GrpcIncomingMessages.FeeType.EXTERNAL_FEE);

    companion object {
        fun getByExternalId(externalId: GrpcIncomingMessages.FeeType): FeeType {
            values()
                .filter { it.externalId == externalId }
                .forEach { return it }
            throw IllegalArgumentException("FeeType (externalId=$externalId) is not found")
        }
    }
}