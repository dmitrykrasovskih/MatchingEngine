package com.lykke.utils.keepalive.http

import com.lykke.utils.AppVersion

class DefaultIsAliveResponseGetter: IsAliveResponseGetter() {
    private val response = IsAliveResponse(AppVersion.VERSION)
    override fun getResponse(): IsAliveResponse {
        return response
    }
}