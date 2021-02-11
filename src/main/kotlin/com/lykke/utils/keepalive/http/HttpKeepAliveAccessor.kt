package com.lykke.utils.keepalive.http

import com.google.gson.Gson
import com.lykke.utils.keepalive.KeepAliveAccessor
import com.lykke.utils.logging.ThrottlingLogger
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

import java.util.Date

internal class HttpKeepAliveAccessor(
        private val path: String,
        private val serviceName: String,
        private val version: String
) : KeepAliveAccessor {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(HttpKeepAliveAccessor::class.java.name)
    }

    private val gson = Gson()

    override fun updateKeepAlive(date: Date, note: String?) {
        sendHttpRequest(KeepAlive(serviceName, version))
    }

    private fun sendHttpRequest(keepAlive: KeepAlive) {
        try {
            val requestConfig = RequestConfig.custom().setConnectTimeout(10 * 1000).build()
            val httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()
            val request = HttpPost(path)
            val params = StringEntity(gson.toJson(keepAlive))
            request.addHeader("content-type", "application/json")
            request.entity = params
            httpClient.execute(request)
        } catch (e: Exception) {
            LOGGER.error("Unable to write log to http: ${e.message}", e)
        }
    }
}