package com.lykke.matching.engine.services.validators.business

import com.lykke.matching.engine.daos.context.ReservedCashInOutContext

interface ReservedCashInOutOperationBusinessValidator {
    fun performValidation(context: ReservedCashInOutContext)
}