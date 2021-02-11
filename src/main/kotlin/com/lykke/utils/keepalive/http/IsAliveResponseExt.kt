package com.lykke.utils.keepalive.http

/** Extended response with 'issueIndicators' field for message sending */
class IsAliveResponseExt(version: String, private var issueIndicators: List<IssueIndicator>) : IsAliveResponse(version)