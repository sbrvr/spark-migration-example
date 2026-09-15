package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, countDistinct, sum, to_date}

import com.databricks.migrationgenie.scalamigrationexample.commonutilities._
import com.databricks.migrationgenie.scalamigrationexample.orderprocessing.Models._

/** Gold layer: join the Silver entities and write analytics-ready Parquet.
  *
  * This is the one layer that is DataFrame-first (spec §2): joins are a DataFrame
  * capability, and the small dimensions are joined with an explicit BROADCAST hint
  * so they are map-side broadcast rather than shuffle-joined.
  *
  * Outputs:
  *   - fact_order_lines             : enriched order line with lineRevenue
  *   - agg_sales_by_region_product  : revenue / quantity / order count per region+product
  */
object GoldProcessing {

  def run(cfg: AppConfig)(implicit spark: SparkSession, sc: SparkContext): Unit = {
    import spark.implicits._

    // --- read Silver as DataFrames --------------------------------------------
    val ordersDf       = spark.read.parquet(s"${cfg.silverPath}/order")
    val orderDetailsDf = spark.read.parquet(s"${cfg.silverPath}/order_details")
    val regionsDf      = spark.read.parquet(s"${cfg.silverPath}/region")
    // Rename the product catalog price so it doesn't collide with order_details.unitPrice.
    val productsDf     = spark.read.parquet(s"${cfg.silverPath}/product")
      .withColumnRenamed("unitPrice", "catalogUnitPrice")

    // Build a marked-up price lookup on the driver (list price + 10%), logged for reference.
    val prices: Map[Int, Double] =
      productsDf.as[(Int, String, String, Double)].rdd // (productId, name, category, catalogUnitPrice)
        .map(t => t._1 -> t._4).collect().toMap
    val markedUp: Map[Int, Double] = prices.mapValues(_ * 1.1)
    println(s"[Gold] marked-up prices (sample): ${markedUp.take(3).toList}")

    // --- broadcast joins: large fact vs. small dimensions (spec §2) -----------
    val fact = orderDetailsDf
      .join(ordersDf, "orderId")                    // fact ⋈ orders on orderId
      .join(broadcast(productsDf), "productId")     // map-side broadcast of products
      .join(broadcast(regionsDf), "regionId")       // map-side broadcast of regions
      .withColumn("lineRevenue", col("quantity") * col("unitPrice"))
      // orderDate: the day part of the (timestamp) orderTs — the grain time-based
      // rollups (daily/weekly/monthly) build on. Kept on the fact for ad-hoc date analysis.
      .withColumn("orderDate", to_date(col("orderTs")))
      .select(
        col("orderDetailId"), col("orderId"), col("productId"), col("regionId"),
        col("quantity"), col("unitPrice"), col("lineRevenue"),
        col("productName"), col("category"), col("catalogUnitPrice"),
        col("regionName"), col("orderTs"), col("orderDate"), col("customerId"), col("status")
      )
      // CACHE: `fact` is consumed twice below (written out, then aggregated). Without a
      // cache the whole join chain (incl. the broadcast joins) recomputes for the agg.
      .cache()

    SparkIO.writeParquet(fact, s"${cfg.goldPath}/fact_order_lines")

    // --- aggregate: DAILY sales by region + product ---------------------------
    // Daily grain (orderDate) is the base for time rollups: weekly => group by
    // weekofyear(orderDate)+year, monthly => group by month(orderDate)+year, etc.
    val agg = fact
      .groupBy(col("orderDate"), col("regionId"), col("regionName"),
        col("productId"), col("productName"))
      .agg(
        sum(col("lineRevenue")).as("totalRevenue"),
        sum(col("quantity")).as("totalQuantity"),
        countDistinct(col("orderId")).as("orderCount")
      )

    SparkIO.writeParquet(agg, s"${cfg.goldPath}/agg_sales_by_date_region_product")

    fact.unpersist() // release the cached fact once both outputs are written
  }
}
