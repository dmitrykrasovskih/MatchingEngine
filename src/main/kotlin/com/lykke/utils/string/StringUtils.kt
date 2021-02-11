package com.lykke.utils.string

import java.util.LinkedList

private fun part(value: String, partIndex: Int, maxPartLength: Int, maxPartCount: Int): String? {
    val fromIndex = partIndex * maxPartLength
    val toIndex = fromIndex + maxPartLength
    return if (value.length <= fromIndex || partIndex > maxPartCount - 1) {
        null
    } else if (value.length < toIndex || partIndex == maxPartCount - 1) {
        value.substring(fromIndex)
    } else {
        value.substring(fromIndex, toIndex)
    }
}

fun String.parts(maxPartLength: Int, maxPartCount: Int): Array<String> {
    var partIndex = 0
    val parts = LinkedList<String>()
    while (true) {
        parts.add(part(this, partIndex++, maxPartLength, maxPartCount) ?: break)
    }
    return parts.toTypedArray()
}