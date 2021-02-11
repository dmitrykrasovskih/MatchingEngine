package com.lykke.utils.files.clean.db

import java.io.File

internal interface LogFilesDatabaseAccessor {
    fun upload(file: File)
}