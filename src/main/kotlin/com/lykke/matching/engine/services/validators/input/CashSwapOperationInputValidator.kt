package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.incoming.parsers.data.CashSwapParsedData

interface CashSwapOperationInputValidator {
    fun performValidation(cashSwapParsedData: CashSwapParsedData)
}