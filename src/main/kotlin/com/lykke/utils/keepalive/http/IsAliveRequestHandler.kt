package com.lykke.utils.keepalive.http

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import org.apache.http.HttpHeaders
import org.apache.logging.log4j.LogManager

internal class IsAliveRequestHandler(private val isAliveResponseGetter: IsAliveResponseGetter) : HttpHandler {

    companion object {
        private val LOGGER = LogManager.getLogger(IsAliveRequestHandler::class.java.name)
    }

    override fun handle(exchange: HttpExchange) {
        try {
            LOGGER.info("Got isAlive request from ${exchange.remoteAddress}")
            exchange.responseHeaders.add(HttpHeaders.CONTENT_TYPE, "application/json")
            val response = isAliveResponseGetter.getResponse()
            val bytes = response.toJson().toByteArray()
            exchange.sendResponseHeaders(response.code, bytes.size.toLong())
            exchange.responseBody.write(bytes)
        } catch (e: Exception) {
            LOGGER.error("Unable to write isAlive response to ${exchange.remoteAddress}", e)
        } finally {
            exchange.close()
        }
    }
}