package com.lykke.matching.engine.config

import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.MatchingEngine
import com.lykke.utils.config.ConfigInitializer

class LocalConfigFactory {
    companion object {
        fun getConfig(): Config {
            return ConfigInitializer.initConfig("local", classOfT = MatchingEngine::class.java).MatchingEngine
        }
    }
}