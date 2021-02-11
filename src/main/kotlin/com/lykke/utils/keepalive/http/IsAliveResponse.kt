package com.lykke.utils.keepalive.http

import com.google.gson.FieldNamingPolicy
import com.google.gson.GsonBuilder
import org.apache.http.HttpStatus

/** Simple response with single 'version' field  */
open class IsAliveResponse(private val version: String,
                           @Transient val code: Int = HttpStatus.SC_OK) {
    companion object {
        private val GSON = GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create()
    }

    internal fun toJson(): String {
        return GSON.toJson(this)
    }
}