package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType

import com.databricks.migrationgenie.scalamigrationexample.commonutilities._
import com.databricks.migrationgenie.scalamigrationexample.orderprocessing.Models._

/** Silver layer: read Bronze, enforce data quality on RDDs, write cleansed Parquet.
  *
  * Policy (spec §6): non-empty is fail-fast; duplicate PKs are deduped (first wins);
  * orphan rows (FK with no parent) are dropped. All metrics are logged.
  *
  * Referential integrity uses BROADCAST parent-key sets so the child RDD is filtered
  * map-side with no shuffle (spec §2 broadcast conventions).
  */
object SilverProcessing {

  def run(cfg: AppConfig)(implicit spark: SparkSession, sc: SparkContext): Unit = {
    import spark.implicits._

    // BOUNDARY(df->rdd): read each Bronze layer back as a DataFrame, then drop to RDD
    // for row-level cleansing (spec §2).
    val regions: RDD[Region]          = spark.read.parquet(s"${cfg.bronzePath}/region").as[Region].rdd
    val products: RDD[Product]        = spark.read.parquet(s"${cfg.bronzePath}/product").as[Product].rdd
    val orders: RDD[Order]            = spark.read.parquet(s"${cfg.bronzePath}/order").as[Order].rdd
    val orderDetails: RDD[OrderDetail] = spark.read.parquet(s"${cfg.bronzePath}/order_details").as[OrderDetail].rdd

    // DQ #1 — non-empty (fail-fast): a missing/empty entity aborts the run.
    Seq(("region", regions.isEmpty()), ("product", products.isEmpty()),
        ("order", orders.isEmpty()), ("order_details", orderDetails.isEmpty()))
      .foreach { case (name, empty) => require(!empty, s"Silver: $name is empty") }

    // DQ #2 — dedupe dimensions by PK (first wins).
    val regionsClean  = DataQualityChecks.dedupeByKey(regions)(_.regionId)
    val productsClean  = DataQualityChecks.dedupeByKey(products)(_.productId)
    val ordersDeduped = DataQualityChecks.dedupeByKey(orders)(_.orderId)

    // BROADCAST: parent PK sets. region/product are tiny; orders is broadcast here at
    // demo scale (spec §2 notes: for prod-scale order volumes, prefer a join instead).
    val regionKeys: Broadcast[Set[Int]]  = sc.broadcast(regionsClean.map(_.regionId).collect().toSet)
    val productKeys: Broadcast[Set[Int]] = sc.broadcast(productsClean.map(_.productId).collect().toSet)

    // DQ #3 — referential integrity: drop orphan orders (regionId with no region).
    val orphanOrders = DataQualityChecks.missingReferences(ordersDeduped, regionKeys)(_.regionId)
    val ordersClean  = DataQualityChecks.dropOrphans(ordersDeduped, regionKeys)(_.regionId)
    println(s"[Silver] orders: ${ordersDeduped.count()} deduped, $orphanOrders orphaned (dropped)")

    val orderKeys: Broadcast[Set[Int]] = sc.broadcast(ordersClean.map(_.orderId).collect().toSet)

    // Dedupe order_details, then drop rows orphaned on EITHER foreign key.
    val odDeduped = DataQualityChecks.dedupeByKey(orderDetails)(_.orderDetailId)
    val odValidOrder = DataQualityChecks.dropOrphans(odDeduped, orderKeys)(_.orderId)
    val orderDetailsClean = DataQualityChecks.dropOrphans(odValidOrder, productKeys)(_.productId)

    // Collect a sample of rejected order_detail rows for logging.
    val buffer = ArrayBuffer.empty[String]
    odDeduped
      .subtract(orderDetailsClean)          // the rejected order_detail rows
      .map(od => s"orderDetailId=${od.orderDetailId} order=${od.orderId} product=${od.productId}")
      .take(20)
      .foreach(buffer += _)
    val badRows: Seq[String] = buffer
    println(s"[Silver] order_details: ${odDeduped.count()} deduped, " +
      s"${orderDetailsClean.count()} kept; sample rejects=${DataQualityChecks.countAll(badRows)}")

    // Assign a surrogate ordinal to each distinct region id (used for logging/keying).
    val distinctRegionIds: Set[Int] = ordersClean.map(_.regionId).collect().toSet
    val regionOrdinal: Map[Int, Int] = distinctRegionIds.iterator.zipWithIndex.toMap
    println(s"[Silver] region ordinals: $regionOrdinal")

    // Write cleansed entities (BOUNDARY rdd->df at the write).
    SparkIO.writeParquet(regionsClean.toDF(), s"${cfg.silverPath}/region")
    SparkIO.writeParquet(productsClean.toDF(), s"${cfg.silverPath}/product")
    // Bronze keeps orderTs RAW (String, spec §5); Silver is the cleansing layer, so it
    // types orderTs to TimestampType here. The generated values are ISO-8601
    // ("yyyy-MM-dd'T'HH:mm:ss"); Spark's String->Timestamp cast parses that form.
    val ordersOut = ordersClean.toDF().withColumn("orderTs", col("orderTs").cast(TimestampType))
    SparkIO.writeParquet(ordersOut, s"${cfg.silverPath}/order")
    SparkIO.writeParquet(orderDetailsClean.toDF(), s"${cfg.silverPath}/order_details")

    // Release broadcasts once Silver is done (spec §2).
    regionKeys.unpersist()
    productKeys.unpersist()
    orderKeys.unpersist()
  }
}
