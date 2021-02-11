package com.lykke.utils.files.clean.db.azure

import com.lykke.utils.azure.getOrCreateBlob
import com.lykke.utils.files.clean.db.LogFilesDatabaseAccessor
import com.microsoft.azure.storage.blob.CloudBlobContainer
import org.apache.log4j.Logger
import java.io.File

internal class AzureLogFilesDatabaseAccessor(connectionString: String, blobContainerName: String) : LogFilesDatabaseAccessor {

    companion object {
        private val LOGGER = Logger.getLogger(AzureLogFilesDatabaseAccessor::class.java.name)
    }

    private val blobContainer: CloudBlobContainer = getOrCreateBlob(connectionString, blobContainerName)

    override fun upload(file: File) {
        LOGGER.debug("Uploading file '${file.name}'")
        blobContainer.getBlockBlobReference(file.name).uploadFromFile(file.absolutePath)
        LOGGER.debug("File '${file.name}' is uploaded")
    }

}