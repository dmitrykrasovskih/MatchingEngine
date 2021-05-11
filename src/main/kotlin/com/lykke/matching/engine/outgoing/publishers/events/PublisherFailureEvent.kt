package com.lykke.matching.engine.outgoing.publishers.events

class PublisherFailureEvent<E>(val publisherName: String, val failedEvent: List<E>?)