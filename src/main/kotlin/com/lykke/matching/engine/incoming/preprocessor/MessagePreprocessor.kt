package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.messages.wrappers.MessageWrapper

interface MessagePreprocessor<WrapperType : MessageWrapper> {
    fun preProcess(messageWrapper: WrapperType)
}