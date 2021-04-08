package com.lykke.matching.engine.services.validators.business.impl

import com.lykke.matching.engine.daos.ReservedCashInOutOperation
import com.lykke.matching.engine.daos.context.ReservedCashInOutContext
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.services.validators.business.ReservedCashInOutOperationBusinessValidator
import com.lykke.matching.engine.services.validators.impl.ValidationException
import com.lykke.matching.engine.utils.NumberUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigDecimal.ZERO

@Component
class ReservedCashInOutOperationBusinessValidatorImpl @Autowired constructor(
    private val balancesHolder: BalancesHolder
) : ReservedCashInOutOperationBusinessValidator {
    companion object {
        private val LOGGER = Logger.getLogger(ReservedCashInOutOperationBusinessValidatorImpl::class.java.name)
    }

    override fun performValidation(context: ReservedCashInOutContext) {
        val operation = context.reservedCashInOutOperation

        if (operation.reservedAmount != ZERO) {
            if (operation.reservedAmount < ZERO) {
                isBalanceValid(operation, operation.reservedAmount)
            } else {
                isReservedVolumeValid(operation, operation.reservedAmount)
            }
        }
        if (operation.reservedSwapAmount != ZERO) {
            if (operation.reservedSwapAmount < ZERO) {
                isBalanceValid(operation, operation.reservedSwapAmount)
            } else {
                isReservedVolumeValid(operation, operation.reservedSwapAmount)
            }
        }
    }

    private fun isReservedVolumeValid(
        operation: ReservedCashInOutOperation,
        reservedAmount: BigDecimal
    ) {
        val accuracy = operation.asset!!.accuracy
        val reservedBalance =
            balancesHolder.getReservedTotalBalance(
                operation.brokerId,
                operation.accountId,
                operation.walletId,
                operation.asset.symbol
            )

        val balance = balancesHolder.getBalance(operation.walletId, operation.asset.symbol)
        if (NumberUtils.setScaleRoundHalfUp(
                balance - reservedBalance - reservedAmount,
                accuracy
            ) < ZERO
        ) {
            LOGGER.info(
                "Reserved cash in operation (${operation.externalId}) for client ${operation.walletId} asset ${operation.asset.symbol}, " +
                        "volume: ${reservedAmount.toPlainString()}: low balance $balance, " +
                        "current reserved balance $reservedBalance"
            )
            throw ValidationException(ValidationException.Validation.RESERVED_VOLUME_HIGHER_THAN_BALANCE)
        }
    }

    private fun isBalanceValid(operation: ReservedCashInOutOperation, reservedAmount: BigDecimal) {
        val accuracy = operation.asset!!.accuracy
        val reservedBalance =
            balancesHolder.getReservedTotalBalance(
                operation.brokerId,
                operation.accountId,
                operation.walletId,
                operation.asset.symbol
            )

        if (NumberUtils.setScaleRoundHalfUp(
                reservedBalance + reservedAmount,
                accuracy
            ) < ZERO
        ) {
            LOGGER.info(
                "Reserved cash out operation (${operation.externalId}) for client ${operation.walletId} asset ${operation.asset.symbol}, " +
                        "volume: ${reservedAmount.toPlainString()}: low reserved balance $reservedBalance"
            )
            throw ValidationException(ValidationException.Validation.LOW_BALANCE)
        }
    }
}