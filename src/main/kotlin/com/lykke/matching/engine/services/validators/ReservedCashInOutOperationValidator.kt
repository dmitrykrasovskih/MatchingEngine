package com.lykke.matching.engine.services.validators

import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages


interface ReservedCashInOutOperationValidator {
    fun performValidation(message: GrpcIncomingMessages.ReservedCashInOutOperation)
}