package com.lykke.matching.engine.config.spring

import com.lykke.matching.engine.utils.config.Config
import com.lykke.utils.AppInitializer
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class LoggerConfig {

    @Autowired
    private lateinit var config: Config

    @Bean(destroyMethod = "")
    fun appStarterLogger(): Logger {
        return Logger.getLogger("AppStarter")
    }

    @Bean
    fun singleLimitOrderPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("SingleLimitOrderPreProcessing")
    }

    @Bean
    fun cashInOutPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashInOutPreProcessing")
    }

    @Bean
    fun reservedCashInOutPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("ReservedCashInOutPreProcessing")
    }

    @Bean
    fun cashTransferPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashTransferPreProcessing")
    }

    @Bean
    fun cashSwapPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("CashSwapPreProcessing")
    }

    @Bean
    fun limitOrderCancelPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("LimitOrderCancelPreProcessing")
    }

    @Bean
    fun limitOrderMassCancelPreProcessingLogger(): ThrottlingLogger {
        return ThrottlingLogger.getLogger("LimitOrderMassCancelPreProcessing")
    }

    @PostConstruct
    fun init() {
        AppInitializer.init()
        ThrottlingLogger.init(config.throttlingLogger)
    }
}