package com.lykke.matching.engine.incoming.listener

import com.lykke.matching.engine.incoming.preprocessor.MessagePreprocessor
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.BlockingQueue
import javax.annotation.PostConstruct

class InputQueueListener<T : MessageWrapper>(
    private val inputQueue: BlockingQueue<T>,
    private val preProcessor: MessagePreprocessor<T>,
    private val logger: ThrottlingLogger,
    threadName: String
) : Thread(threadName) {

    companion object {
        private const val ERROR_MESSAGE = "Unable to pre process message"
    }

    @PostConstruct
    fun init() = start()

    override fun run() {
        while (true) {
            try {
                preProcessor.preProcess(inputQueue.take())
            } catch (e: Exception) {
                logger.error(ERROR_MESSAGE, e)
            }
        }
    }
}