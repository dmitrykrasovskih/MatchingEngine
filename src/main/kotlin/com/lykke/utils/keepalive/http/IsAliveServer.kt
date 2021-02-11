package com.lykke.utils.keepalive.http

import com.lykke.utils.logging.ThrottlingLogger
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress

internal class IsAliveServer(private val port: Int, private val isAliveResponseGetter: IsAliveResponseGetter) : Thread() {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(IsAliveServer::class.java.name)
        private val PATH = "/api/IsAlive"
    }

    private fun connect(): Boolean {
        LOGGER.info("Starting http server: port: $port, path: $PATH")
        return try {
            val server = HttpServer.create()
            server.bind(InetSocketAddress(port), 0)
            server.createContext(PATH, IsAliveRequestHandler(isAliveResponseGetter))
            server.start()
            LOGGER.info("Started http server: port: $port, path: $PATH")
            true
        } catch (e: Exception) {
            LOGGER.error("Unable start http server: port: $port, path: $PATH", e)
            false
        }
    }

    override fun run() {
        while (!connect()) {
            sleep(1000)
        }
    }
}