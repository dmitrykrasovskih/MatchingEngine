package com.lykke.matching.engine.daos.fee

import com.lykke.matching.engine.daos.FeeInstruction
import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import java.math.BigDecimal

open class NewFeeInstruction(type: FeeType,
                             takerSizeType: FeeSizeType?,
                             takerSize: BigDecimal?,
                             sourceWalletId: String?,
                             targetWalletId: String?,
                             val assetIds: List<String>) : FeeInstruction(type, takerSizeType, takerSize, sourceWalletId, targetWalletId) {

    companion object {
        fun create(fees: List<GrpcIncomingMessages.Fee>): List<NewFeeInstruction> {
            return fees.map { create(it) }
        }

        fun create(fee: GrpcIncomingMessages.Fee): NewFeeInstruction {
            val feeType = FeeType.getByExternalId(fee.type)
            var sizeType: FeeSizeType? = if (fee.sizeType != null) FeeSizeType.getByExternalId(fee.sizeType) else null
            if (feeType != FeeType.NO_FEE && sizeType == null) {
                sizeType = FeeSizeType.PERCENTAGE
            }
            return NewFeeInstruction(
                    feeType,
                    sizeType,
                    if (fee.hasSize()) BigDecimal(fee.size.value) else null,
                    if (fee.hasSourceWalletId()) fee.sourceWalletId.value else null,
                    if (fee.hasTargetWalletId()) fee.targetWalletId.value else null,
                    fee.assetIdList.toList()
            )
        }
    }

    override fun toString(): String {
        return "NewFeeInstruction(type=$type" +
                (if (sizeType != null) ", sizeType=$sizeType" else "") +
                (if (size != null) ", size=${size.toPlainString()}" else "") +
                (if (assetIds.isNotEmpty()) ", assetIds=$assetIds" else "") +
                (if (sourceWalletId?.isNotEmpty() == true) ", sourceWalletId=$sourceWalletId" else "") +
                "${if (targetWalletId?.isNotEmpty() == true) ", targetWalletId=$targetWalletId" else ""})"
    }

    override fun toNewFormat() = this
}