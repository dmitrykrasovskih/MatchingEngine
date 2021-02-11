package com.lykke.utils.notification

interface Listener<T> {
    fun subscribe(subscriber: Subscriber<T>)
    fun unsubscribe(subscriber: Subscriber<T>)
}