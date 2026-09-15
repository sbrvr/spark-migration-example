package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.databricks.migrationgenie.scalamigrationexample.commonutilities._

/** End-to-end local pipeline test. Runs Bronze -> Silver -> Gold in local mode
  * against generated sample data, and reconciles the Gold outputs. Exercises the
  * dependency on the `common` module (FileChecker, ConfigLoader, DataQualityChecks). */
class EndToEndLocalPipelineSpec extends FunSuite with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _
  private implicit var sc: SparkContext = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("EndToEndLocalPipelineSpec").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("pipeline runs in local mode and Gold reconciles with Silver") {
    val cfg = ConfigLoader.load()
    assert(cfg.isLocal, "test must run under a local master")

    SampleDataGenerator.generateIfMissing(cfg)
    BronzeIngestion.run(cfg)
    SilverProcessing.run(cfg)
    GoldProcessing.run(cfg)

    val fact     = spark.read.parquet(s"${cfg.goldPath}/fact_order_lines")
    val silverOd = spark.read.parquet(s"${cfg.silverPath}/order_details")
    val agg      = spark.read.parquet(s"${cfg.goldPath}/agg_sales_by_date_region_product")

    // Inner joins keep every valid Silver order-detail row.
    assert(fact.count() == silverOd.count())
    assert(fact.count() > 0L)

    // Silver typed orderTs from String -> Timestamp; it carries into Gold.
    assert(fact.schema("orderTs").dataType.typeName == "timestamp")

    // Aggregated revenue must reconcile with the fact-level revenue.
    val factRevenue = fact.agg(sum("lineRevenue")).first().getDouble(0)
    val aggRevenue  = agg.agg(sum("totalRevenue")).first().getDouble(0)
    assert(math.abs(factRevenue - aggRevenue) < 1e-6)
  }
}
