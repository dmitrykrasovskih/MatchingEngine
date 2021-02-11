package com.lykke.matching.engine.daos.fee.v2

import com.lykke.matching.engine.daos.FeeSizeType
import com.lykke.matching.engine.daos.FeeType
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import java.math.BigDecimal

class NewLimitOrderFeeInstruction(
        type: FeeType,
        takerSizeType: FeeSizeType?,
        takerSize: BigDecimal?,
        val makerSizeType: FeeSizeType?,
        val makerSize: BigDecimal?,
        sourceWalletId: String?,
        targetWalletId: String?,
        assetIds: List<String>,
        val makerFeeModificator: BigDecimal?
) : NewFeeInstruction(type, takerSizeType, takerSize, sourceWalletId, targetWalletId, assetIds) {

    companion object {
        fun create(fees: List<GrpcIncomingMessages.LimitOrderFee>): List<NewLimitOrderFeeInstruction> {
            return fees.map { create(it) }
        }

        fun create(fee: GrpcIncomingMessages.LimitOrderFee): NewLimitOrderFeeInstruction {
            val feeType = FeeType.getByExternalId(fee.type)
            var takerSizeType: FeeSizeType? = if (fee.takerSizeType != null) FeeSizeType.getByExternalId(fee.takerSizeType) else null
            var makerSizeType: FeeSizeType? = if (fee.makerSizeType != null) FeeSizeType.getByExternalId(fee.makerSizeType) else null
            if (feeType != FeeType.NO_FEE) {
                if (takerSizeType == null) {
                    takerSizeType = FeeSizeType.PERCENTAGE
                }
                if (makerSizeType == null) {
                    makerSizeType = FeeSizeType.PERCENTAGE
                }
            }
            return NewLimitOrderFeeInstruction(
                    feeType,
                    takerSizeType,
                    if (fee.hasTakerSize()) BigDecimal(fee.takerSize.value) else null,
                    makerSizeType,
                    if (fee.hasMakerSize()) BigDecimal(fee.makerSize.value) else null,
                    if (fee.hasSourceWalletId()) fee.sourceWalletId.value else null,
                    if (fee.hasTargetWalletId()) fee.targetWalletId.value else null,
                    fee.assetIdList.toList(),
                    if (fee.hasMakerFeeModificator())  BigDecimal(fee.makerFeeModificator.value) else null)
        }
    }

    override fun toString(): String {
        return "NewLimitOrderFeeInstruction(type=$type" +
                (if (sizeType != null) ", takerSizeType=$sizeType" else "") +
                (if (size != null) ", takerSize=${size.toPlainString()}" else "") +
                (if (makerSizeType != null) ", makerSizeType=$makerSizeType" else "") +
                (if (makerSize != null) ", makerSize=${makerSize.toPlainString()}" else "") +
                (if (makerFeeModificator != null) ", makerFeeModificator=${makerFeeModificator.toPlainString()}" else "") +
                (if (assetIds.isNotEmpty() == true) ", assetIds=$assetIds" else "") +
                (if (sourceWalletId?.isNotEmpty() == true) ", sourceWalletId=$sourceWalletId" else "") +
                "${if (targetWalletId?.isNotEmpty() == true) ", targetWalletId=$targetWalletId" else ""})"
    }

}