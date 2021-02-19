package com.lykke.utils.files.clean

import com.lykke.utils.files.clean.config.LogFilesCleanerConfig
import com.lykke.utils.files.clean.config.LogFilesCleanerParams
import com.lykke.utils.files.clean.db.azure.AzureLogFilesDatabaseAccessor
import org.apache.logging.log4j.LogManager
import java.io.*
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream
import kotlin.concurrent.fixedRateTimer

class LogFilesCleaner private constructor(private val params: LogFilesCleanerParams) {

    companion object {
        private val LOGGER = LogManager.getLogger(LogFilesCleaner::class.java.name)
        internal const val ARCHIVE_FILE_NAME_SUFFIX = ".gz"

        private fun startWithParams(params: LogFilesCleanerParams) {
            val uploader = LogFilesCleaner(params)
            fixedRateTimer("LogFilesCleaner", true, 0, params.period) {
                try {
                    uploader.process()
                } catch (e: Exception) {
                    LOGGER.error("Unable to check and upload files (directory: ${params.directory}): ${e.message}", e)
                }
            }
            LOGGER.debug("LogFilesCleaner timer started")
        }

        fun start(config: LogFilesCleanerConfig) {
            if (!config.enabled) {
                return
            }

            val period = config.period ?: TimeUnit.DAYS.toMillis(1)
            if (config.directory == null) {
                throw IllegalArgumentException("directory is null")
            }

            if (config.uploadDaysThreshold != null) {
                if (config.connectionString == null) {
                    throw IllegalArgumentException("connectionString is null")
                }
                if (config.blobContainerName == null) {
                    throw IllegalArgumentException("blobContainerName is null")
                }
                startWithParams(
                    LogFilesCleanerParams(
                        period,
                        config.directory,
                        AzureLogFilesDatabaseAccessor(config.connectionString, config.blobContainerName),
                        config.archiveDaysThreshold,
                        config.uploadDaysThreshold
                    )
                )
                return
            }

            if (config.archiveDaysThreshold != null) {
                startWithParams(
                    LogFilesCleanerParams(
                        period,
                        config.directory,
                        archiveDaysThreshold = config.archiveDaysThreshold
                    )
                )
                return
            }

            LOGGER.error("LogFileCleaner is enabled but there are no configured thresholds")
        }
    }

    private fun process() {
        LOGGER.debug("Starting process, path: ${params.directory}")
        val directory = File(params.directory)
        if (!directory.exists()) {
            LOGGER.error("Directory '${params.directory}' is not exist")
            return
        }

        val allFiles = directory.listFiles()
        LOGGER.debug("All files: ${allFiles.size}")
        if (params.filterToArchive != null) {
            archiveFiles(directory)
        }
        if (params.filterToUpload != null) {
            uploadFiles(directory)
        }
        LOGGER.debug("Processed")
    }

    private fun archiveFiles(directory: File) {
        val filesToArchive = directory.listFiles(params.filterToArchive)
        LOGGER.debug(
            "Files filtered to archive and delete: ${filesToArchive.size} ${
                if (filesToArchive.isNotEmpty()) filesToArchive.map { it.name }.toString() else ""
            }"
        )

        filesToArchive.forEach { file ->
            val archived = try {
                archiveFile(file)
                true
            } catch (e: Exception) {
                LOGGER.error("Unable to archive file '${file.name}'", e)
                false
            }

            if (archived && !file.delete()) {
                LOGGER.error("Unable to delete file '${file.name}'")
            }
        }

    }

    private fun uploadFiles(directory: File) {
        val filesToUpload = directory.listFiles(params.filterToUpload)
        LOGGER.debug(
            "Files filtered to upload and delete: ${filesToUpload.size} ${
                if (filesToUpload.isNotEmpty()) filesToUpload.map { it.name }.toString() else ""
            }"
        )

        filesToUpload.forEach { file ->
            val uploaded = try {
                params.databaseAccessor!!.upload(file)
                true
            } catch (e: Exception) {
                LOGGER.error("Unable to upload file '${file.name}'", e)
                false
            }

            if (uploaded && !file.delete()) {
                LOGGER.error("Unable to delete file '${file.name}'")
            }
        }
    }

    private fun archiveFile(file: File) {
        LOGGER.debug("Archiving file '${file.name}'")
        var fis: InputStream? = null
        var fos: OutputStream? = null
        var gzipOS: OutputStream? = null
        try {
            val archivedFileName = file.absolutePath + ARCHIVE_FILE_NAME_SUFFIX
            if (File(archivedFileName).exists()) {
                throw IllegalArgumentException("File '$archivedFileName' already exists")
            }
            fis = FileInputStream(file)
            fos = FileOutputStream(archivedFileName)
            gzipOS = GZIPOutputStream(fos)
            val buffer = ByteArray(1024)
            var len = fis.read(buffer)
            while (len != -1) {
                gzipOS.write(buffer, 0, len)
                len = fis.read(buffer)
            }
            LOGGER.debug("File '${file.name}' is archived")
        } finally {
            gzipOS?.close()
            fos?.close()
            fis?.close()
        }
    }

}