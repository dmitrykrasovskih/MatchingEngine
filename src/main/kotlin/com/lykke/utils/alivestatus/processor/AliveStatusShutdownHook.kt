package com.lykke.utils.alivestatus.processor

internal class AliveStatusShutdownHook(private val processor: AliveStatusProcessor) : Thread() {
    init {
        this.name = "AliveStatusShutdownHook"
    }

    override fun run() {
        processor.unlock()
    }
}