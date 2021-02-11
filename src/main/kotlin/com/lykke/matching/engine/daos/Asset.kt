package com.lykke.matching.engine.daos

class Asset(
    val brokerId: String,
    val symbol: String,
    val accuracy: Int
){
    override fun toString(): String {
        return "Asset(" +
                "brokerId='$brokerId', " +
                "symbol='$symbol', " +
                "accuracy=$accuracy)"
    }
}