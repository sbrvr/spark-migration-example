package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import java.io.{File, PrintWriter}
import java.net.URI

import scala.util.Random

import org.apache.spark.SparkContext

import com.databricks.migrationgenie.scalamigrationexample.commonutilities._

/** Generates a small, referentially-consistent CSV dataset for LOCAL runs and tests.
  *
  * This is test-fixture bootstrapping, not pipeline I/O, so it writes plain local
  * files via `java.io` (it does not touch Spark or the Hadoop FS API). It only runs
  * in local mode; the file paths come from the resolved `file://…` config paths.
  *
  * Sizes (spec §4): 4 regions, 10 products, 50 orders, 150 order details.
  */
object SampleDataGenerator {

  private val rng = new Random(42) // fixed seed => deterministic sample data

  /** Generate all four CSVs if the order file is absent. No-op if inputs exist. */
  def generateIfMissing(cfg: AppConfig)(implicit sc: SparkContext): Unit = {
    if (FileChecker.exists(cfg.orderPath)) {
      println(s"[SampleData] inputs already present at ${cfg.orderPath} — skipping generation")
    } else {
      println("[SampleData] generating sample CSVs under the configured local paths")
      writeRegions(cfg.regionPath)
      writeProducts(cfg.productPath)
      writeOrders(cfg.orderPath)
      writeOrderDetails(cfg.orderDetailsPath)
    }
  }

  // --- writers ---------------------------------------------------------------

  private def writeRegions(fileUri: String): Unit =
    write(fileUri, "regionId,regionName",
      Seq("1,North", "2,South", "3,East", "4,West"))

  private def writeProducts(fileUri: String): Unit = {
    val cats = Array("Electronics", "Home", "Toys", "Grocery")
    val rows = (1 to 10).map { pid =>
      val price = 5.0 + rng.nextInt(9500) / 100.0 // 5.00 .. 99.99
      s"$pid,Product_$pid,${cats(rng.nextInt(cats.length))},$price"
    }
    write(fileUri, "productId,productName,category,unitPrice", rows)
  }

  private def writeOrders(fileUri: String): Unit = {
    val statuses = Array("NEW", "SHIPPED", "CANCELLED", "RETURNED")
    val rows = (1 to 50).map { oid =>
      val regionId   = 1 + rng.nextInt(4)
      val customerId = 1000 + rng.nextInt(200)
      val status     = statuses(rng.nextInt(statuses.length))
      val ts         = f"2024-01-${1 + rng.nextInt(28)}%02dT10:00:00" // ISO-8601, zero-padded day
      s"$oid,$ts,$regionId,$customerId,$status"
    }
    write(fileUri, "orderId,orderTs,regionId,customerId,status", rows)
  }

  private def writeOrderDetails(fileUri: String): Unit = {
    val rows = (1 to 150).map { odid =>
      val orderId   = 1 + rng.nextInt(50)   // FK into orders (1..50)
      val productId = 1 + rng.nextInt(10)   // FK into products (1..10)
      val quantity  = 1 + rng.nextInt(10)
      val unitPrice = 5.0 + rng.nextInt(9500) / 100.0
      s"$odid,$orderId,$productId,$quantity,$unitPrice"
    }
    write(fileUri, "orderDetailId,orderId,productId,quantity,unitPrice", rows)
  }

  /** Write a header + rows to the local file named by a `file://` URI. */
  private def write(fileUri: String, header: String, rows: Seq[String]): Unit = {
    val file = new File(new URI(fileUri))
    Option(file.getParentFile).foreach(_.mkdirs())
    val pw = new PrintWriter(file)
    try {
      pw.println(header)
      rows.foreach(pw.println)
    } finally {
      pw.close()
    }
    println(s"[SampleData] wrote ${rows.size} rows -> ${file.getAbsolutePath}")
  }
}
