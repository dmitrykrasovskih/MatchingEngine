package com.lykke.utils.notification

interface Subscriber<T> {
    fun notify(message: T)
}