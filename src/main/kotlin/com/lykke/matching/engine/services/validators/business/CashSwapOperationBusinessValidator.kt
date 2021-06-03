package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.context.CashSwapContext

interface CashSwapOperationBusinessValidator {
    fun performValidation(cashSwapContext: CashSwapContext)
}