package com.lykke.matching.engine.grpc

import com.lykke.matching.engine.LOGGER
import io.grpc.stub.StreamObserver

class TestStreamObserver<T>: StreamObserver<T> {
    val responses = mutableListOf<T>()

    override fun onNext(value: T) {
        responses.add(value)
    }

    override fun onError(t: Throwable) {
        LOGGER.error("Error: " + t.message, t)
    }

    override fun onCompleted() {
        LOGGER.info("Completed")
    }
}