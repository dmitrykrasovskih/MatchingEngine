package com.lykke.matching.engine.database.file

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.NewLimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.utils.logging.ThrottlingLogger
import org.nustaq.serialization.FSTConfiguration
import java.math.BigDecimal
import java.nio.file.*
import java.util.*
import java.util.stream.Collectors

open class AbstractFileOrderBookDatabaseAccessor(
    private val ordersDir: String,
    logPrefix: String = ""
) {

    companion object {
        private val LOGGER = ThrottlingLogger.getLogger(AbstractFileOrderBookDatabaseAccessor::class.java.name)
        private const val PREV_ORDER_BOOK_FILE_PREFIX = "_prev_"
    }

    private val logPrefix = if (logPrefix.isNotEmpty()) "$logPrefix " else ""
    private var conf = FSTConfiguration.createDefaultConfiguration()

    init {
        Files.createDirectories(Paths.get(ordersDir))
    }

    fun loadOrdersFromFiles(): List<LimitOrder> {
        var result: List<LimitOrder> = ArrayList()

        try {
            val orderDirPath = Paths.get(ordersDir)
            if (Files.notExists(orderDirPath)) {
                return result
            }

            result = Files.list(orderDirPath)
                .filter { path ->
                    !Files.isDirectory(path) &&
                            !path
                                .fileName
                                .toString()
                                .startsWith(PREV_ORDER_BOOK_FILE_PREFIX)
                }
                .flatMap { readOrderBookFileOrPrevFileOnFail(it).stream() }
                .collect(Collectors.toList())


        } catch (e: Exception) {
            val message = "Unable to load ${logPrefix}limit orders"
            LOGGER.error(message, e)
        }

        LOGGER.info("Loaded ${result.size} active ${logPrefix}limit orders")
        return result
    }

    private fun readOrderBookFileOrPrevFileOnFail(filePath: Path): List<LimitOrder> {
        return try {
            readFile(filePath)
        } catch (e: Exception) {
            LOGGER.error(
                "Unable to read ${logPrefix}order book file ${filePath.fileName}. Trying to load previous one",
                e
            )
            readPrevOrderBookFile(filePath.fileName.toString())
        }
    }

    private fun readPrevOrderBookFile(fileName: String): List<LimitOrder> {
        try {
            return readFile(getPrevOrderBookFilePath(fileName))
        } catch (e: Exception) {
            LOGGER.error("Unable to read previous ${logPrefix}order book file $fileName.", e)
        }

        return Collections.emptyList()
    }

    private fun getPrevOrderBookFilePath(fileName: String): Path {
        return Paths.get(ordersDir, "$PREV_ORDER_BOOK_FILE_PREFIX$fileName")
    }

    private fun getOrderBookFilePath(fileName: String): Path {
        return Paths.get(ordersDir, fileName)
    }

    private fun getOrderBookFileName(asset: String, buy: Boolean): String {
        return "${asset}_$buy"
    }

    protected fun updateOrdersFile(asset: String, buy: Boolean, orders: Collection<LimitOrder>) {
        try {
            val fileName = getOrderBookFileName(asset, buy)
            archiveAndDeleteFile(fileName)
            saveFile(fileName, orders.toList())
        } catch (e: Exception) {
            val message = "Unable to save ${logPrefix}order book, size: ${orders.size}"
            LOGGER.error(message, e)
        }
    }

    private fun readFile(filePath: Path): List<LimitOrder> {
        val bytes = Files.readAllBytes(filePath)
        val orders = conf.asObject(bytes)

        if (orders is List<*>) {
            return convertOrders(orders)
        }

        return LinkedList()
    }

    private fun convertOrders(orders: List<*>): List<LimitOrder> {
        var oldFormatOrdersCount = 0
        val convertedOrders = orders.stream()
            .filter { it is LimitOrder || it is NewLimitOrder }
            .map {
                if (it is LimitOrder) {
                    it
                } else {
                    oldFormatOrdersCount++
                    fromNewLimitOrderToLimitOrder(it as NewLimitOrder)
                }
            }
            .collect(Collectors.toCollection { LinkedList<LimitOrder>() })

        if (oldFormatOrdersCount != 0) {
            LOGGER.info("Old format orders count: $oldFormatOrdersCount for asset pair: ${convertedOrders.first.assetPairId}")
        }

        return convertedOrders
    }

    private fun fromNewLimitOrderToLimitOrder(order: NewLimitOrder): LimitOrder {
        @Suppress("UNCHECKED_CAST")
        return LimitOrder(
            order.id,
            order.externalId,
            order.assetPairId,
            order.brokerId,
            order.accountId,
            order.clientId,
            BigDecimal.valueOf(order.volume),
            BigDecimal.valueOf(order.price),
            order.status,
            order.statusDate,
            order.createdAt,
            order.registered,
            BigDecimal.valueOf(order.remainingVolume),
            order.lastMatchTime,
            order.reservedLimitVolume?.toBigDecimal(),
            convertLimitOrderFeeInstructions(order.fees as List<com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction>?),
            order.type,
            order.lowerLimitPrice?.toBigDecimal(),
            order.lowerPrice?.toBigDecimal(),
            order.upperLimitPrice?.toBigDecimal(),
            order.upperPrice?.toBigDecimal(),
            order.previousExternalId,
            null,
            null,
            null,
            null
        )
    }

    private fun convertLimitOrderFeeInstructions(fees: List<com.lykke.matching.engine.daos.fee.NewLimitOrderFeeInstruction>?): List<NewLimitOrderFeeInstruction>? {
        if (fees == null) {
            return null
        }
        return fees
            .stream()
            .map {
                NewLimitOrderFeeInstruction(
                    it.type, it.sizeType,
                    it.size, it.makerSizeType,
                    it.makerSize, it.sourceWalletId,
                    it.targetWalletId, it.assetIds, it.makerFeeModificator
                )
            }
            .collect(Collectors.toList())
    }

    private fun saveFile(fileName: String, data: List<LimitOrder>) {
        try {
            val bytes = conf.asByteArray(data)
            Files.write(getOrderBookFilePath(fileName), bytes, StandardOpenOption.CREATE)
        } catch (e: Exception) {
            val message = "Unable to save order book file, name: $fileName"
            LOGGER.error(message, e)
            throw e
        }
    }

    private fun archiveAndDeleteFile(fileName: String) {
        try {
            val prevOrderBookFile = getPrevOrderBookFilePath(fileName)
            val orderBookFile = getOrderBookFilePath(fileName)
            Files.move(orderBookFile, prevOrderBookFile, StandardCopyOption.REPLACE_EXISTING)
        } catch (e: NoSuchFileException) {
            // it is new order book - ignore
        } catch (e: Exception) {
            val message = "Unable to archive and delete, name: $fileName"
            LOGGER.error(message, e)
        }
    }
}