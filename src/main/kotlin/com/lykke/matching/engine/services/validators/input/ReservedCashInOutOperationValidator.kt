package com.lykke.matching.engine.services.validators.input

import com.lykke.matching.engine.incoming.parsers.data.ReservedCashInOutParsedData

interface ReservedCashInOutOperationValidator {
    fun performValidation(reservedCashInOutParsedData: ReservedCashInOutParsedData)
}