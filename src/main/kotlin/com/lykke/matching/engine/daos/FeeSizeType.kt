package com.lykke.matching.engine.daos

import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages


enum class FeeSizeType constructor(val externalId: GrpcIncomingMessages.FeeSizeType) {

    PERCENTAGE(GrpcIncomingMessages.FeeSizeType.PERCENTAGE),
    ABSOLUTE(GrpcIncomingMessages.FeeSizeType.ABSOLUTE);

    companion object {
        fun getByExternalId(externalId: GrpcIncomingMessages.FeeSizeType): FeeSizeType {
            values()
                .filter { it.externalId == externalId }
                .forEach { return it }
            throw IllegalArgumentException("FeeTypeSize (externalId=$externalId) is not found")
        }
    }
}