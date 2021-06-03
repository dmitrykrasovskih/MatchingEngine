package com.lykke.matching.engine.services.validators.input.impl

import com.lykke.matching.engine.holders.ApplicationSettingsHolder
import com.lykke.matching.engine.incoming.parsers.data.CashSwapParsedData
import com.lykke.matching.engine.messages.wrappers.CashSwapOperationMessageWrapper
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.services.validators.input.CashSwapOperationInputValidator
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class CashSwapOperationInputValidatorImpl @Autowired constructor(private val applicationSettingsHolder: ApplicationSettingsHolder) :
    CashSwapOperationInputValidator {

    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationInputValidatorImpl::class.java.name)
    }

    override fun performValidation(cashSwapParsedData: CashSwapParsedData) {
        isAssetExist(cashSwapParsedData)
        isAssetEnabled(cashSwapParsedData)
        isVolumeAccuracyValid(cashSwapParsedData)
    }

    private fun isAssetExist(cashSwapParsedData: CashSwapParsedData) {
        val cashSwapContext = getCashSwapContext(cashSwapParsedData)

        if (cashSwapContext.swapOperation.asset1 == null || cashSwapContext.swapOperation.asset2 == null) {
            val swapOperation = cashSwapContext.swapOperation
            LOGGER.info(
                "Cash swap operation (${swapOperation.externalId}) from client ${swapOperation.walletId1}, asset ${cashSwapParsedData.assetId1} " +
                        "to client ${swapOperation.walletId2}, asset ${cashSwapParsedData.assetId2} " +
                        ": unknown asset"
            )
            throw ValidationException(ValidationException.Validation.UNKNOWN_ASSET)
        }
    }

    private fun isAssetEnabled(cashSwapParsedData: CashSwapParsedData) {
        val cashSwapContext = getCashSwapContext(cashSwapParsedData)

        if (applicationSettingsHolder.isAssetDisabled(cashSwapContext.swapOperation.asset1!!.symbol) || applicationSettingsHolder.isAssetDisabled(
                cashSwapContext.swapOperation.asset2!!.symbol
            )
        ) {
            val swapOperation = cashSwapContext.swapOperation
            LOGGER.info(
                "Cash swap operation (${swapOperation.externalId}) from client ${swapOperation.walletId1}, asset ${cashSwapParsedData.assetId1} " +
                        "to client ${swapOperation.walletId2}, asset ${cashSwapParsedData.assetId2} " + ": disabled asset"
            )
            throw ValidationException(ValidationException.Validation.DISABLED_ASSET)
        }
    }

    private fun isVolumeAccuracyValid(cashSwapParsedData: CashSwapParsedData) {
        val cashSwapContext = getCashSwapContext(cashSwapParsedData)

        val swapOperation = cashSwapContext.swapOperation
        var volumeValid = NumberUtils.isScaleSmallerOrEqual(
            swapOperation.volume1,
            swapOperation.asset1!!.accuracy
        )

        if (!volumeValid) {
            LOGGER.info(
                "Volume accuracy invalid fromClient  ${swapOperation.walletId1}, " +
                        "to client ${swapOperation.walletId2} assetId: ${cashSwapParsedData.assetId1}, volume: ${swapOperation.volume1}"
            )
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }

        volumeValid = NumberUtils.isScaleSmallerOrEqual(
            swapOperation.volume2,
            swapOperation.asset2!!.accuracy
        )

        if (!volumeValid) {
            LOGGER.info(
                "Volume accuracy invalid fromClient  ${swapOperation.walletId1}, " +
                        "to client ${swapOperation.walletId2} assetId: ${cashSwapParsedData.assetId2}, volume: ${swapOperation.volume2}"
            )
            throw ValidationException(ValidationException.Validation.INVALID_VOLUME_ACCURACY)
        }
    }

    private fun getCashSwapContext(cashSwapParsedData: CashSwapParsedData) =
        (cashSwapParsedData.messageWrapper as CashSwapOperationMessageWrapper).context!!
}