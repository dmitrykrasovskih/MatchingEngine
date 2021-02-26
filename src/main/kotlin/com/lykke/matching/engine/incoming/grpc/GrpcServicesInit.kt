package com.lykke.matching.engine.incoming.grpc

import com.lykke.matching.engine.AppInitialData
import com.lykke.matching.engine.holders.AssetsHolder
import com.lykke.matching.engine.holders.AssetsPairsHolder
import com.lykke.matching.engine.holders.BalancesHolder
import com.lykke.matching.engine.messages.MessageProcessor
import com.lykke.matching.engine.messages.wrappers.*
import com.lykke.matching.engine.services.GenericLimitOrderService
import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppVersion
import com.lykke.utils.logging.MetricsLogger
import com.lykke.utils.logging.ThrottlingLogger
import io.grpc.ServerBuilder
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.util.concurrent.BlockingQueue

@Component
class GrpcServicesInit(
    private val messageProcessor: MessageProcessor,
    private val appInitialData: AppInitialData
) : Runnable {

    @Autowired
    private lateinit var config: Config

    @Autowired
    private lateinit var cashInOutInputQueue: BlockingQueue<CashInOutOperationMessageWrapper>

    @Autowired
    private lateinit var cashTransferInputQueue: BlockingQueue<CashTransferOperationMessageWrapper>

    @Autowired
    private lateinit var limitOrderInputQueue: BlockingQueue<SingleLimitOrderMessageWrapper>

    @Autowired
    private lateinit var limitOrderCancelInputQueue: BlockingQueue<LimitOrderCancelMessageWrapper>

    @Autowired
    private lateinit var preProcessedMessageQueue: BlockingQueue<MessageWrapper>

    @Autowired
    private lateinit var balancesHolder: BalancesHolder

    @Autowired
    private lateinit var genericLimitOrderService: GenericLimitOrderService

    @Autowired
    private lateinit var assetsHolder: AssetsHolder

    @Autowired
    private lateinit var assetsPairsHolder: AssetsPairsHolder

    @Autowired
    private lateinit var registry: MeterRegistry

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(GrpcServicesInit::class.java.name)
    }

    override fun run() {
        messageProcessor.start()

        MetricsLogger.getLogger().logWarning(
            "Spot.${config.matchingEngine.name} ${AppVersion.VERSION} : " +
                    "Started : ${appInitialData.ordersCount} orders, ${appInitialData.stopOrdersCount} " +
                    "stop orders,${appInitialData.balancesCount} " +
                    "balances for ${appInitialData.clientsCount} clients"
        )

        LOGGER.info("Starting gRpc services")

        with(config.matchingEngine.grpcEndpoints) {
            ServerBuilder.forPort(cashApiServicePort)
                .addService(CashApiService(cashInOutInputQueue, cashTransferInputQueue, registry)).build().start()
            LOGGER.info("Started CashApiService at $cashApiServicePort port")

            ServerBuilder.forPort(tradingApiServicePort).addService(
                TradingApiService(
                    limitOrderInputQueue,
                    limitOrderCancelInputQueue,
                    preProcessedMessageQueue,
                    registry
                )
            ).build().start()
            LOGGER.info("Started TradingApiService at $tradingApiServicePort port")

            ServerBuilder.forPort(balancesServicePort)
                .addService(BalancesService(balancesHolder, registry)).build().start()
            LOGGER.info("Started BalancesService at $balancesServicePort port")

            ServerBuilder.forPort(orderBooksServicePort)
                .addService(OrderBooksService(genericLimitOrderService, assetsHolder, assetsPairsHolder, registry))
                .build()
                .start()
            LOGGER.info("Started OrderBooksService at $orderBooksServicePort port")
        }
    }
}