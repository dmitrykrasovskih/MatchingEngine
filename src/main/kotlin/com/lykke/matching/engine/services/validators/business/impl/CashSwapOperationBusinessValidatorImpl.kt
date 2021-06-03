package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.Asset
import com.lykke.matching.engine.daos.context.CashSwapContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.business.CashSwapOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class CashSwapOperationBusinessValidatorImpl(private val balancesHolder: BalancesHolder) :
    CashSwapOperationBusinessValidator {
    companion object {
        private val LOGGER = Logger.getLogger(CashSwapOperationBusinessValidatorImpl::class.java.name)

    }

    override fun performValidation(cashSwapContext: CashSwapContext) {
        val swapOperation = cashSwapContext.swapOperation
        validateBalanceValid(
            swapOperation.matchingEngineOperationId,
            swapOperation.asset1!!,
            swapOperation.brokerId,
            swapOperation.accountId1,
            swapOperation.walletId1,
            swapOperation.volume1
        )
        validateBalanceValid(
            swapOperation.matchingEngineOperationId,
            swapOperation.asset2!!,
            swapOperation.brokerId,
            swapOperation.accountId2,
            swapOperation.walletId2,
            swapOperation.volume2
        )
    }

    private fun validateBalanceValid(
        id: String,
        asset: Asset,
        brokerId: String,
        accountId: String,
        walletId: String,
        volume: BigDecimal
    ) {
        val reservedBalanceOfFromClient = balancesHolder.getReservedFoSwapBalance(
            brokerId,
            accountId,
            walletId,
            asset.symbol
        )
        if (reservedBalanceOfFromClient < volume) {
            LOGGER.info(
                "Cash swap operation ($id) wallet $walletId " +
                        "asset $asset, " +
                        "volume: ${NumberUtils.roundForPrint(volume)}: " +
                        "low balance"
            )

            throw ValidationException(
                ValidationException.Validation.LOW_BALANCE,
                "ClientId:$walletId, asset:${asset.symbol}, volume:${NumberUtils.roundForPrint(volume)}"
            )
        }
    }
}