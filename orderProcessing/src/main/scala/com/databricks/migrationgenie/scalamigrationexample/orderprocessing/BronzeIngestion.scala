package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import scala.collection.breakOut

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

import com.databricks.migrationgenie.scalamigrationexample.commonutilities._
import com.databricks.migrationgenie.scalamigrationexample.orderprocessing.Models._

/** Bronze layer: raw, typed ingest with NO data-quality enforcement.
  *
  *  - `order` / `order_details` are always read from files (existence-checked first).
  *  - `product` / `region` come from files (local) or Postgres (prod), per config.
  *  - Every entity is written to `bronze/<entity>` as Parquet.
  *
  * RDD-first (spec §2): sources are parsed into `RDD[T]`; the DataFrame API is used
  * only at the write boundary (RDD -> toDF -> parquet) and for the JDBC read.
  */
object BronzeIngestion {

  // Postgres source queries live in code (they are domain logic, not deployment config).
  // Each is a wrapped subquery for Spark's JDBC `dbtable`. Aliases are QUOTED so Postgres
  // preserves the exact camelCase column names the case-class encoder expects (unquoted
  // identifiers fold to lowercase).
  private val ProductQuery: String =
    """(SELECT product_id AS "productId", product_name AS "productName",
      |        category AS "category", unit_price AS "unitPrice"
      | FROM product) AS p""".stripMargin

  private val RegionQuery: String =
    """(SELECT region_id AS "regionId", region_name AS "regionName" FROM region) AS r""".stripMargin

  def run(cfg: AppConfig)(implicit spark: SparkSession, sc: SparkContext): Unit = {
    import spark.implicits._ // toDF / Dataset encoders

    // --- order & order_details: file sources, existence-checked ---------------
    // FileChecker is the ONLY Hadoop FS usage, and only for existence (spec §5).
    FileChecker.assertExists(cfg.orderPath)
    FileChecker.assertExists(cfg.orderDetailsPath)

    val orders: RDD[Order]        = readOrders(cfg.orderPath)
    val orderDetails: RDD[OrderDetail] = readOrderDetails(cfg.orderDetailsPath)

    // --- product & region: file (local) or jdbc (prod) ------------------------
    val products: RDD[Product] = cfg.productSource match {
      case "file" => readProducts(cfg.productPath)
      case "jdbc" =>
        val pg = cfg.postgres.getOrElse(sys.error("jdbc product source requires Postgres config"))
        // BOUNDARY(df->rdd): JDBC yields a DataFrame with Spark types inferred from the
        // Postgres column types; coerce to the exact case-class schema before .as[Product].
        toProducts(new PostgresReader(pg).readQuery(ProductQuery))
      case other  => sys.error(s"unknown product source: $other")
    }

    val regions: RDD[Region] = cfg.regionSource match {
      case "file" => readRegions(cfg.regionPath)
      case "jdbc" =>
        val pg = cfg.postgres.getOrElse(sys.error("jdbc region source requires Postgres config"))
        toRegions(new PostgresReader(pg).readQuery(RegionQuery))
      case other  => sys.error(s"unknown region source: $other")
    }

    // Map each order to its region in a single pass (logged as a sanity check).
    val orderToRegion: Map[Int, Int] =
      orders.collect().map(o => o.orderId -> o.regionId)(breakOut)
    println(s"[Bronze] ingested ${orderToRegion.size} distinct order->region mappings")

    // BOUNDARY(rdd->df): write each RDD as Parquet via toDF (spec §2).
    SparkIO.writeParquet(orders.toDF(), s"${cfg.bronzePath}/order")
    SparkIO.writeParquet(orderDetails.toDF(), s"${cfg.bronzePath}/order_details")
    SparkIO.writeParquet(products.toDF(), s"${cfg.bronzePath}/product")
    SparkIO.writeParquet(regions.toDF(), s"${cfg.bronzePath}/region")
  }

  // --- JDBC -> exact case-class schema ----------------------------------------
  // Spark infers DataFrame types from the Postgres column types, which vary by DDL
  // (e.g. `numeric` -> DecimalType, `bigint` -> LongType). `.as[T]` performs a STRICT
  // up-cast and would fail on such mismatches (e.g. "cannot up cast unitPrice from
  // decimal to double"). We resolve each column by name (case-insensitive) and apply an
  // EXPLICIT cast to the target type — explicit casts are permissive — so the resulting
  // schema exactly matches the case class regardless of the underlying Postgres types.
  // Column names come from the quoted aliases in the JDBC queries (see ProductQuery / RegionQuery).
  //
  // NULL handling: fields typed as primitives (Int/Double) cannot hold null in the case
  // class, so a null from a nullable Postgres column would fail at row encoding. We drop
  // rows null in those primitive columns (via `na.drop`) and log the count. String columns
  // may remain null (the encoder allows null strings), so they are not part of the subset.

  private def toProducts(df: DataFrame)(implicit spark: SparkSession): RDD[Product] = {
    import spark.implicits._
    // cache so the null-count and the downstream read share a single JDBC scan.
    val typed = df.select(
      col("productId").cast(IntegerType).as("productId"),
      col("productName").cast(StringType).as("productName"),
      col("category").cast(StringType).as("category"),
      col("unitPrice").cast(DoubleType).as("unitPrice")
    ).cache()
    val nonNull = typed.na.drop(Seq("productId", "unitPrice")) // primitive columns
    val dropped = typed.count() - nonNull.count()
    if (dropped > 0) println(s"[Bronze] product: dropped $dropped row(s) with null productId/unitPrice")
    nonNull.as[Product].rdd
  }

  private def toRegions(df: DataFrame)(implicit spark: SparkSession): RDD[Region] = {
    import spark.implicits._
    val typed = df.select(
      col("regionId").cast(IntegerType).as("regionId"),
      col("regionName").cast(StringType).as("regionName")
    ).cache()
    val nonNull = typed.na.drop(Seq("regionId")) // primitive column
    val dropped = typed.count() - nonNull.count()
    if (dropped > 0) println(s"[Bronze] region: dropped $dropped row(s) with null regionId")
    nonNull.as[Region].rdd
  }

  // --- CSV parsing (RDD API): drop the header line, split on comma ------------
  // Sample files are comma-separated with a single header row (spec §3/§4).

  private def bodyLines(path: String)(implicit sc: SparkContext): RDD[Array[String]] = {
    val raw = sc.textFile(path)
    val header = raw.first()                 // header row to skip
    raw.filter(_ != header).map(_.split(",", -1))
  }

  private def readRegions(path: String)(implicit sc: SparkContext): RDD[Region] =
    bodyLines(path).map(c => Region(c(0).toInt, c(1)))

  private def readProducts(path: String)(implicit sc: SparkContext): RDD[Product] =
    bodyLines(path).map(c => Product(c(0).toInt, c(1), c(2), c(3).toDouble))

  private def readOrders(path: String)(implicit sc: SparkContext): RDD[Order] =
    bodyLines(path).map(c => Order(c(0).toInt, c(1), c(2).toInt, c(3).toInt, c(4)))

  private def readOrderDetails(path: String)(implicit sc: SparkContext): RDD[OrderDetail] =
    bodyLines(path).map(c => OrderDetail(c(0).toInt, c(1).toInt, c(2).toInt, c(3).toInt, c(4).toDouble))
}
