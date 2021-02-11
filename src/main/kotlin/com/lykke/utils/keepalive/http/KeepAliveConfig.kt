package com.lykke.utils.keepalive.http

data class KeepAliveConfig(
        /** Passive mode flag */
        val passive: Boolean,
        /** App name */
        val name: String,
        /** Active mode request sending period */
        val interval: Long?,
        /** Active mode request address */
        val path: String?,
        /** Port for passive mode incoming requests listening */
        val port: Int?
)