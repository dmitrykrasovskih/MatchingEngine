package com.lykke.utils.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

internal object ResourceFileConfigParser {
    fun <Config> initConfig(fileName: String, classOfT: Class<Config>): Config {
        val mapper = ObjectMapper(YAMLFactory())
        mapper.findAndRegisterModules()
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        return mapper.readValue(
            ResourceFileConfigParser::class.java.classLoader.getResource(fileName).readText(),
            classOfT
        )
    }
}
