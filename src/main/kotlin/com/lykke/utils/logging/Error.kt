package com.lykke.utils.logging

import com.google.gson.Gson

internal class Error(private val Type: String, private val Sender: String, private val Message: String): LoggableObject {

    override fun toString(): String = "Error(Sender='$Sender', Type='$Type', Error='$Message')"

    override fun getJson(): String = Gson().toJson(this)
}