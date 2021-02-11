package com.lykke.utils.alivestatus.processor


internal interface AliveStatusProcessorCreator {
    fun createProcessor(): AliveStatusProcessor
}