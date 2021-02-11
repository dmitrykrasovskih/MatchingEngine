package com.lykke.matching.engine.incoming.parsers.data

import com.lykke.matching.engine.daos.fee.v2.NewFeeInstruction
import com.lykke.matching.engine.messages.wrappers.MessageWrapper

class CashTransferParsedData(messageWrapper: MessageWrapper,
                             val assetId: String,
                             val feeInstructions: List<NewFeeInstruction>): ParsedData(messageWrapper)