package com.lykke.matching.engine.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.lykke.matching.engine.utils.config.Config
import com.lykke.matching.engine.utils.config.MatchingEngine
import org.apache.logging.log4j.LogManager
import org.springframework.core.env.Environment
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import javax.naming.ConfigurationException

class HttpConfigFactory {

    companion object {
        private val LOGGER = LogManager.getLogger("AppStarter")

        fun getConfig(environment: Environment): Config {
            val commangLineArgs = environment.getProperty("nonOptionArgs", Array<String>::class.java)

            if (commangLineArgs == null) {
                LOGGER.error("Not enough args. Usage: httpConfigString")
                throw IllegalArgumentException("Not enough args. Usage: httpConfigString")
            }

            return downloadConfig(commangLineArgs[0])
        }

        private fun downloadConfig(httpString: String): Config {
            val cfgUrl = URL(httpString)
            val connection = cfgUrl.openConnection()
            val inputStream = BufferedReader(InputStreamReader(connection.inputStream))

            try {
                val response = StringBuilder()
                var inputLine = inputStream.readLine()

                while (inputLine != null) {
                    response.append(inputLine).append("\n")
                    inputLine = inputStream.readLine()
                }

                inputStream.close()

                val mapper = ObjectMapper(YAMLFactory())
                mapper.findAndRegisterModules()
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true)
                return mapper.readValue(
                    response.toString(),
                    MatchingEngine::class.java
                ).MatchingEngine
            } catch (e: Exception) {
                throw ConfigurationException("Unable to read config from $httpString: ${e.message}")
            } finally {
                inputStream.close()
            }
        }
    }
}