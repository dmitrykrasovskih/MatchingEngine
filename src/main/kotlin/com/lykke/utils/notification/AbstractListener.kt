package com.lykke.utils.notification

import com.lykke.utils.logging.ThrottlingLogger
import java.util.concurrent.CopyOnWriteArraySet

abstract class AbstractListener<T> : Listener<T> {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AbstractListener::class.java.name)
    }

    private val subscribers = CopyOnWriteArraySet<Subscriber<T>>()

    override fun subscribe(subscriber: Subscriber<T>) {
        subscribers.add(subscriber)
    }

    override fun unsubscribe(subscriber: Subscriber<T>) {
        subscribers.remove(subscriber)
    }

    protected fun notifySubscribers(message: T) {
        subscribers.forEach { subscriber ->
            try {
                subscriber.notify(message)
            } catch (e: Throwable) {
                LOGGER.error("Got error during notifying", e)
                handleNotNotifiedSubscriber(subscriber, message)
            }
        }
    }

    protected open fun handleNotNotifiedSubscriber(subscriber: Subscriber<T>, message: T) {
        // do nothing in default implementation
    }
}