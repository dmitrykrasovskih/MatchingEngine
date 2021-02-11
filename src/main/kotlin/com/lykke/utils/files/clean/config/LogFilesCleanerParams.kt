package com.lykke.utils.files.clean.config

import com.lykke.utils.files.clean.DaysThresholdFileFilter
import com.lykke.utils.files.clean.db.LogFilesDatabaseAccessor
import java.io.FileFilter

internal open class LogFilesCleanerParams(val period: Long,
                                          val directory: String,
                                          val databaseAccessor: LogFilesDatabaseAccessor? = null,
                                          archiveDaysThreshold: Int? = null,
                                          uploadDaysThreshold: Int? = null) {

    val filterToArchive: FileFilter?
    val filterToUpload: FileFilter?

    init {
        if (archiveDaysThreshold != null && uploadDaysThreshold != null && archiveDaysThreshold > uploadDaysThreshold) {
            throw IllegalArgumentException("archiveDaysThreshold ($archiveDaysThreshold) shouldn't be greater than uploadDaysThreshold ($uploadDaysThreshold)")
        }
        if (uploadDaysThreshold != null && databaseAccessor == null) {
            throw IllegalArgumentException("databaseAccessor is null")
        }
        filterToArchive = if (archiveDaysThreshold != null) DaysThresholdFileFilter(archiveDaysThreshold, false) else null
        filterToUpload = if (uploadDaysThreshold != null) DaysThresholdFileFilter(uploadDaysThreshold, archiveDaysThreshold != null) else null
    }
}