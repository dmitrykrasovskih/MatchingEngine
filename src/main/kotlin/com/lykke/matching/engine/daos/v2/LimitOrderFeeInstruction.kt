package com.lykke.matching.engine.daos.v2

import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import java.math.BigDecimal

class LimitOrderFeeInstruction(
    type: FeeType,
    takerSizeType: FeeSizeType?,
    takerSize: BigDecimal?,
    val makerSizeType: FeeSizeType?,
    val makerSize: BigDecimal?,
    sourceClientId: String?,
    targetClientId: String?
) : FeeInstruction(type, takerSizeType, takerSize, sourceClientId, targetClientId) {

    companion object {
        fun create(fee: GrpcIncomingMessages.LimitOrderFee?): LimitOrderFeeInstruction? {
            if (fee == null) {
                return null
            }
            val feeType = FeeType.getByExternalId(fee.type)
            var takerSizeType: FeeSizeType? =
                if (fee.takerSizeType != null) FeeSizeType.getByExternalId(fee.takerSizeType) else null
            var makerSizeType: FeeSizeType? =
                if (fee.makerSizeType != null) FeeSizeType.getByExternalId(fee.makerSizeType) else null
            if (feeType != FeeType.NO_FEE) {
                if (takerSizeType == null) {
                    takerSizeType = FeeSizeType.PERCENTAGE
                }
                if (makerSizeType == null) {
                    makerSizeType = FeeSizeType.PERCENTAGE
                }
            }
            return LimitOrderFeeInstruction(
                feeType,
                takerSizeType,
                if (fee.hasTakerSize()) BigDecimal(fee.takerSize.value) else null,
                makerSizeType,
                if (fee.hasMakerSize()) BigDecimal(fee.makerSize.value) else null,
                if (fee.hasSourceWalletId()) fee.sourceWalletId.value else null,
                if (fee.hasTargetWalletId()) fee.targetWalletId.value else null
            )
        }
    }

    override fun toString(): String {
        return "LimitOrderFeeInstruction(type=$type" +
                (if (sizeType != null) ", takerSizeType=$sizeType" else "") +
                (if (size != null) ", takerSize=${size.toPlainString()}" else "") +
                (if (makerSizeType != null) ", makerSizeType=$makerSizeType" else "") +
                (if (makerSize != null) ", makerSize=${makerSize.toPlainString()}" else "") +
                (if (sourceWalletId?.isNotEmpty() == true) ", sourceWalletId=$sourceWalletId" else "") +
                "${if (targetWalletId?.isNotEmpty() == true) ", targetWalletId=$targetWalletId" else ""})"
    }

    override fun toNewFormat() = NewLimitOrderFeeInstruction(
        type,
        sizeType,
        size,
        makerSizeType,
        makerSize,
        sourceWalletId,
        targetWalletId,
        emptyList(),
        null
    )

}