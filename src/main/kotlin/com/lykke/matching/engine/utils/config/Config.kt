package com.lykke.matching.engine.utils.config

import com.lykke.utils.logging.config.SlackNotificationConfig
import com.lykke.utils.logging.config.ThrottlingLoggerConfig

data class Config(
    val matchingEngine: MatchingEngineConfig,
    val slackNotifications: SlackNotificationConfig,
    val throttlingLogger: ThrottlingLoggerConfig
)