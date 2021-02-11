package com.lykke.utils.alivestatus.daos

import java.text.SimpleDateFormat
import java.util.Date

internal data class AliveStatus(
        private val startTime: Date,
        private val lastAliveTime: Date,
        private val ip: String,
        private val running: Boolean
) {
    override fun toString(): String {
        val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        return "(startTime=${sdf.format(startTime)}, lastAliveTime=${sdf.format(lastAliveTime)}, ip='$ip', running=$running)"
    }
}