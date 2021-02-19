package com.lykke.utils

import org.apache.logging.log4j.LogManager
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object AppInitializer {

    private val LOGGER = LogManager.getLogger("AppStarter")

    fun init() {
        val startTime = LocalDateTime.now()
        teeLog("Application launched at " + startTime.format(DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")))
        teeLog("Revision-number: " + AppVersion.REVISION_NUMBER)
        teeLog("Build-number: " + AppVersion.BUILD_NUMBER)
        teeLog("Version: " + AppVersion.VERSION)
        teeLog("Working-dir: " + File(".").absolutePath)
        teeLog("Java-Info: " + System.getProperty("java.vm.name") + " (" + System.getProperty("java.version") + ")")
    }

    fun teeLog(message: String) {
        println(message)
        LOGGER.info(message)
    }
}