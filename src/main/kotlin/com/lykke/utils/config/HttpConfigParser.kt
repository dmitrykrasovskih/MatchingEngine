package com.lykke.utils.config

import com.google.gson.FieldNamingPolicy.UPPER_CAMEL_CASE
import com.google.gson.GsonBuilder
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import javax.naming.ConfigurationException

internal object HttpConfigParser {
    fun <Config> initConfig(httpString: String, classOfT: Class<Config>): Config {
        try {
            val cfgUrl = URL(httpString)
            val connection = cfgUrl.openConnection()
            val inputStream = BufferedReader(InputStreamReader(connection.inputStream))

            val response = StringBuilder()
            var inputLine = inputStream.readLine()

            while (inputLine != null) {
                response.append(inputLine)
                inputLine = inputStream.readLine()
            }

            inputStream.close()

            val gson = GsonBuilder().setFieldNamingPolicy(UPPER_CAMEL_CASE).create()
            return gson.fromJson(response.toString(), classOfT)
        } catch (e: Exception) {
            throw ConfigurationException("Unable to read config from $httpString: ${e.message}")
        }
    }
}