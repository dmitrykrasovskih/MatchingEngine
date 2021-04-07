package com.lykke.matching.engine.services.validators.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class ReservedCashInOutOperationValidatorImpl @Autowired constructor(
    private val assetsHolder: AssetsHolder,
    private val balancesHolder: BalancesHolder
) : ReservedCashInOutOperationValidator {
    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(message: GrpcIncomingMessages.ReservedCashInOutOperation) {
        isVolumesAccuracyValid(message)

        if (!message.reservedVolume.isNullOrEmpty()) {
            if (BigDecimal(message.reservedVolume) < BigDecimal.ZERO) {
                isBalanceValid(message, BigDecimal(message.reservedVolume))
            } else {
                isReservedVolumeValid(message, BigDecimal(message.reservedVolume))
            }
        }
        if (!message.reservedForSwapVolume.isNullOrEmpty()) {
            if (BigDecimal(message.reservedForSwapVolume) < BigDecimal.ZERO) {
                isBalanceValid(message, BigDecimal(message.reservedForSwapVolume))
            } else {
                isReservedVolumeValid(message, BigDecimal(message.reservedForSwapVolume))
            }
        }
    }

    private fun isReservedVolumeValid(
        message: GrpcIncomingMessages.ReservedCashInOutOperation,
        reservedAmount: BigDecimal
    ) {
        val accuracy = assetsHolder.getAsset(message.assetId).accuracy
        val reservedBalance =
            balancesHolder.getReservedTotalBalance(
                message.brokerId,
                message.accountId,
                message.walletId,
                message.assetId
            )

        val balance = balancesHolder.getBalance(message.walletId, message.assetId)
        if (NumberUtils.setScaleRoundHalfUp(
                balance - reservedBalance - reservedAmount,
                accuracy
            ) < BigDecimal.ZERO
        ) {
            LOGGER.info(
                "Reserved cash in operation (${message.id}) for client ${message.walletId} asset ${message.assetId}, " +
                        "volume: ${reservedAmount.toPlainString()}: low balance $balance, " +
                        "current reserved balance $reservedBalance"
            )
            throw ValidationException(ValidationException.Validation.RESERVED_VOLUME_HIGHER_THAN_BALANCE)
        }
    }

    private fun isBalanceValid(message: GrpcIncomingMessages.ReservedCashInOutOperation, reservedAmount: BigDecimal) {
        val accuracy = assetsHolder.getAsset(message.assetId).accuracy
        val reservedBalance =
            balancesHolder.getReservedTotalBalance(
                message.brokerId,
                message.accountId,
                message.walletId,
                message.assetId
            )

        if (NumberUtils.setScaleRoundHalfUp(
                reservedBalance + reservedAmount,
                accuracy
            ) < BigDecimal.ZERO
        ) {
            LOGGER.info(
                "Reserved cash out operation (${message.id}) for client ${message.walletId} asset ${message.assetId}, " +
                        "volume: ${reservedAmount.toPlainString()}: low reserved balance $reservedBalance"
            )
            throw ValidationException(ValidationException.Validation.LOW_BALANCE)
        }
    }

    private fun isVolumesAccuracyValid(message: GrpcIncomingMessages.ReservedCashInOutOperation) {
        val asset = assetsHolder.getAsset(message.assetId)
        isVolumeAccuracyValid(message.reservedVolume, asset)
        isVolumeAccuracyValid(message.reservedForSwapVolume, asset)
    }

    private fun isVolumeAccuracyValid(volume: String?, asset: Asset) {
        if (!volume.isNullOrEmpty()) {
            val volumeValid = NumberUtils.isScaleSmallerOrEqual(BigDecimal(volume), asset.accuracy)

            if (!volumeValid) {
                LOGGER.info("Volume accuracy invalid, assetId ${asset.symbol}, volume $volume")
                throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
            }
        }
    }
}