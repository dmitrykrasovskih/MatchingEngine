package com.lykke.matching.engine

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class Application {
    @Autowired
    lateinit var clientsRequestsSocketServer: Runnable
    @Autowired
    lateinit var grpcServicesInit: Runnable

    fun run () {
        clientsRequestsSocketServer.run()
        grpcServicesInit.run()
    }
}