package com.lykke.matching.engine.outgoing.messages.v2.events.common

import com.lykke.matching.engine.outgoing.messages.v2.enums.FeeSizeType
import com.lykke.matching.engine.outgoing.messages.v2.enums.FeeType
import com.myjetwallet.messages.outgoing.grpc.OutgoingMessages

class FeeInstruction(
    val type: FeeType,
    val size: String?,
    val sizeType: FeeSizeType?,
    val makerSize: String?,
    val makerSizeType: FeeSizeType?,
    val sourceWalletId: String?,
    val targetWalletId: String?,
    val assetsIds: List<String>?,
    val makerFeeModificator: String?,
    val index: Int
) : EventPart<OutgoingMessages.FeeInstruction.Builder> {

    override fun createGeneratedMessageBuilder(): OutgoingMessages.FeeInstruction.Builder {
        val builder = OutgoingMessages.FeeInstruction.newBuilder()
        builder.type = OutgoingMessages.FeeInstruction.FeeType.forNumber(type.id)
        size?.let {
            builder.setSize(it)
        }
        sizeType?.let {
            builder.sizeType = OutgoingMessages.FeeInstruction.FeeSizeType.forNumber(it.id)
        }
        makerSize?.let {
            builder.makerSize = it
        }
        makerSizeType?.let {
            builder.makerSizeType = OutgoingMessages.FeeInstruction.FeeSizeType.forNumber(it.id)
        }
        sourceWalletId?.let {
            builder.sourceWalletId = it
        }
        targetWalletId?.let {
            builder.targetWalletId = it
        }
        assetsIds?.let {
            builder.addAllAssetsIds(it)
        }
        makerFeeModificator?.let {
            builder.makerFeeModificator = it
        }
        builder.index = index
        return builder
    }

}