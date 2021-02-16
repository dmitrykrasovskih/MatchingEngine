package com.lykke.matching.engine.incoming.preprocessor

import com.lykke.matching.engine.daos.context.SingleLimitOrderContext
import com.lykke.matching.engine.grpc.TestStreamObserver
import com.lykke.matching.engine.holders.MessageProcessingStatusHolder
import com.lykke.matching.engine.incoming.parsers.ContextParser
import com.lykke.matching.engine.incoming.parsers.data.SingleLimitOrderParsedData
import com.lykke.matching.engine.incoming.parsers.impl.SingleLimitOrderContextParser
import com.lykke.matching.engine.messages.MessageStatus
import com.lykke.matching.engine.messages.wrappers.MessageWrapper
import com.lykke.matching.engine.messages.wrappers.SingleLimitOrderMessageWrapper
import com.lykke.matching.engine.utils.MessageBuilder
import com.lykke.utils.logging.ThrottlingLogger
import com.myjetwallet.messages.incoming.grpc.GrpcIncomingMessages
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class AbstractMessagePreprocessorTest {

//    class TestParsedData(messageWrapper: MessageWrapper) : ParsedData(messageWrapper)

    class TestMessagePreprocessor(
        contextParser: ContextParser<SingleLimitOrderParsedData, SingleLimitOrderMessageWrapper>,
        messageProcessingStatusHolder: MessageProcessingStatusHolder,
        preProcessedMessageQueue: BlockingQueue<MessageWrapper>,
        private val preProcessSuccess: Boolean
    ) :
        AbstractMessagePreprocessor<SingleLimitOrderParsedData, SingleLimitOrderMessageWrapper>(
            contextParser,
            messageProcessingStatusHolder,
            preProcessedMessageQueue,
            ThrottlingLogger.getLogger(TestMessagePreprocessor::class.java.name)
        ) {

        override fun preProcessParsedData(parsedData: SingleLimitOrderParsedData): Boolean {
            return preProcessSuccess
        }

        override fun writeResponse(
            messageWrapper: SingleLimitOrderMessageWrapper,
            status: MessageStatus,
            message: String?
        ) {
            messageWrapper.writeResponse(status, message)
        }
    }

    private lateinit var queue: BlockingQueue<MessageWrapper>
    private lateinit var clientHandler: TestStreamObserver<GrpcIncomingMessages.LimitOrderResponse>
    private lateinit var messageWrapper: SingleLimitOrderMessageWrapper

    @Before
    fun setUp() {
        queue = LinkedBlockingQueue<MessageWrapper>()
        clientHandler = TestStreamObserver()
        messageWrapper = SingleLimitOrderMessageWrapper(
            GrpcIncomingMessages.LimitOrder.getDefaultInstance(),
            clientHandler,
            true,
            context = SingleLimitOrderContext.Builder().messageId("MessageID")
                .limitOrder(MessageBuilder.buildLimitOrder()).build()
        )
    }

    private fun createStatusHolder(
        isMessageProcessingEnabled: Boolean,
        isHealthStatusOk: Boolean
    ): MessageProcessingStatusHolder {
        val statusHolder = Mockito.mock(MessageProcessingStatusHolder::class.java)
        Mockito.`when`(statusHolder.isMessageProcessingEnabled())
            .thenReturn(isMessageProcessingEnabled)
        Mockito.`when`(statusHolder.isHealthStatusOk())
            .thenReturn(isHealthStatusOk)
        return statusHolder
    }

    private fun createPreprocessor(
        isMessageProcessingEnabled: Boolean,
        isHealthStatusOk: Boolean,
        preProcessSuccess: Boolean = true
    ): TestMessagePreprocessor {
        val contextParser =
            Mockito.mock(SingleLimitOrderContextParser::class.java) { SingleLimitOrderParsedData(messageWrapper, "") }
                    as SingleLimitOrderContextParser
        return TestMessagePreprocessor(
            contextParser,
            createStatusHolder(isMessageProcessingEnabled, isHealthStatusOk),
            queue,
            preProcessSuccess
        )
    }

    @Test
    fun testPreProcess() {
        val messagePreprocessor = createPreprocessor(true, true)

        messagePreprocessor.preProcess(messageWrapper)

        assertNotNull(messageWrapper.messagePreProcessorStartTimestamp)
        assertNotNull(messageWrapper.messagePreProcessorEndTimestamp)
        assertEquals(0, clientHandler.responses.size)
        assertEquals(1, queue.size)
        assertEquals(messageWrapper, queue.poll())
    }

    @Test
    fun testPreProcessWithMaintenanceMode() {
        val messagePreprocessor = createPreprocessor(true, false)

        messagePreprocessor.preProcess(messageWrapper)

        assertNotNull(messageWrapper.messagePreProcessorStartTimestamp)
        assertNotNull(messageWrapper.messagePreProcessorEndTimestamp)
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertEquals(MessageStatus.RUNTIME.type, response.status.number)

        assertEquals(0, queue.size)
    }

    @Test
    fun testPreProcessWithDisabledMessageProcessing() {
        val messagePreprocessor = createPreprocessor(false, true)

        messagePreprocessor.preProcess(messageWrapper)

        assertNotNull(messageWrapper.messagePreProcessorStartTimestamp)
        assertNotNull(messageWrapper.messagePreProcessorEndTimestamp)
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertEquals(MessageStatus.MESSAGE_PROCESSING_DISABLED.type, response.status.number)

        assertEquals(0, queue.size)
    }

    @Test
    fun testPreProcessWithDisabledMessageProcessingAndMaintenanceMode() {
        val messagePreprocessor = createPreprocessor(false, false)

        messagePreprocessor.preProcess(messageWrapper)

        assertNotNull(messageWrapper.messagePreProcessorStartTimestamp)
        assertNotNull(messageWrapper.messagePreProcessorEndTimestamp)
        assertEquals(1, clientHandler.responses.size)
        val response = clientHandler.responses.single()
        assertEquals(MessageStatus.MESSAGE_PROCESSING_DISABLED.type, response.status.number)

        assertEquals(0, queue.size)
    }

    @Test
    fun testFailPreProcess() {
        val messagePreprocessor = createPreprocessor(true, true, false)

        messagePreprocessor.preProcess(messageWrapper)

        assertNotNull(messageWrapper.messagePreProcessorStartTimestamp)
        assertNotNull(messageWrapper.messagePreProcessorEndTimestamp)
        assertEquals(0, clientHandler.responses.size)
        assertEquals(0, queue.size)
    }
}
