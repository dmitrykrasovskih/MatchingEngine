package com.lykke.matching.engine.web.common

import com.lykke.utils.logging.ThrottlingLogger
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class GlobalControllerAdvice {

    private companion object {
        val LOGGER = ThrottlingLogger.getLogger(GlobalControllerAdvice::class.java.name)
    }

    @ExceptionHandler
    fun handleException(e: Exception): ResponseEntity<*> {
        val message = "Unhandled exception occurred in web component:\n${e.message}"
        LOGGER.error(message, e)

        return ResponseEntity(message, HttpStatus.INTERNAL_SERVER_ERROR)
    }
}