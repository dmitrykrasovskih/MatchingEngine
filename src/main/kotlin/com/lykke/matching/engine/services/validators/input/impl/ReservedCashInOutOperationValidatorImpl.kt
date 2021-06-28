package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.ReservedCashInOutOperation
import com.lykke.matching.engine.daos.context.ReservedCashInOutContext
import com.lykke.matching.engine.incoming.parsers.data.ReservedCashInOutParsedData
import com.lykke.matching.engine.messages.wrappers.ReservedCashInOutOperationMessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.ReservedCashInOutOperationValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigDecimal.ZERO

@Component
class ReservedCashInOutOperationValidatorImpl @Autowired constructor(
) : ReservedCashInOutOperationValidator {
    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationValidatorImpl::class.java.name)
    }

    override fun performValidation(reservedCashInOutParsedData: ReservedCashInOutParsedData) {
        val context =
            (reservedCashInOutParsedData.messageWrapper as ReservedCashInOutOperationMessageWrapper).context as ReservedCashInOutContext

        val operation = context.reservedCashInOutOperation
        isAssetExist(context, reservedCashInOutParsedData.assetId)
        isVolumesAccuracyValid(operation)
    }

    private fun isAssetExist(reservedCashInOutContext: ReservedCashInOutContext, inputAssetId: String) {
        if (reservedCashInOutContext.reservedCashInOutOperation.asset == null) {
            LOGGER.info(
                "Asset with id: $inputAssetId does not exist, cash in/out operation; ${reservedCashInOutContext.reservedCashInOutOperation.externalId}), " +
                        "for client ${reservedCashInOutContext.reservedCashInOutOperation.walletId}"
            )
            throw ValidationException(ValidationException.Validation.UNKNOWN_ASSET)
        }
    }

    private fun isVolumesAccuracyValid(operation: ReservedCashInOutOperation) {
        isVolumeAccuracyValid(operation.reservedAmount, operation.asset!!)
        isVolumeAccuracyValid(operation.reservedSwapAmount, operation.asset)
    }

    private fun isVolumeAccuracyValid(volume: BigDecimal, asset: Asset) {
        if (volume != ZERO) {
            val volumeValid = NumberUtils.isScaleSmallerOrEqual(volume, asset.accuracy)

            if (!volumeValid) {
                LOGGER.info("Volume accuracy invalid, assetId ${asset.symbol}, volume $volume")
                throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
            }
        }
    }
}