package com.lykke.utils.files.clean

import org.apache.logging.log4j.LogManager
import java.io.File
import java.io.FileFilter
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.TimeUnit

internal class DaysThresholdFileFilter(thresholdDays: Int, applyToArchive: Boolean) : FileFilter {

    private companion object {
        private val LOGGER = LogManager.getLogger(DaysThresholdFileFilter::class.java.name)
    }

    private val thresholdMs = TimeUnit.DAYS.toMillis(thresholdDays.toLong())
    private val suffix = if (applyToArchive) LogFilesCleaner.ARCHIVE_FILE_NAME_SUFFIX else ""

    override fun accept(pathname: File?): Boolean {
        val file = pathname ?: return false
        if (file.isDirectory) {
            return false
        }

        val parsedFileName = if (suffix.isNotEmpty()) {
            if (!file.name.endsWith(suffix)) {
                return false
            }
            file.name.substringBeforeLast(suffix)
        } else {
            file.name
        }

        if (parsedFileName.endsWith(".log") || suffix.isEmpty() && parsedFileName.endsWith(LogFilesCleaner.ARCHIVE_FILE_NAME_SUFFIX)) {
            return false
        }

        val date = try {
            parseDate(parsedFileName)
        } catch (e: ParseException) {
            LOGGER.debug("'${file.name}' is skipped: ${e.message}")
            return false
        }
        return Date().time - date.time > thresholdMs
    }

    private fun parseDate(fileName: String): Date {
        val dateFormat = SimpleDateFormat("yyyy-MM-dd")
        val fileType = fileName.split(".").toList().last()
        return if (fileType == "log") dateFormat.parse(dateFormat.format(Date())) else SimpleDateFormat("yyyy-MM-dd").parse(
            fileType
        )
    }

    init {
        if (thresholdDays < 0) {
            throw IllegalArgumentException("thresholdDays is negative: $thresholdDays")
        }
    }
}