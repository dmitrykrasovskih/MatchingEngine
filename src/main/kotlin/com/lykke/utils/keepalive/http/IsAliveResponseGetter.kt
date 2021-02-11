package com.lykke.utils.keepalive.http

abstract class IsAliveResponseGetter {
    abstract fun getResponse(): IsAliveResponse
}