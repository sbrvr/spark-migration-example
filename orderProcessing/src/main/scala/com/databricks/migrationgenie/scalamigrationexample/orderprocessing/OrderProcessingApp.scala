package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.databricks.migrationgenie.scalamigrationexample.commonutilities._

/** Entry point: orchestrates Bronze -> Silver -> Gold.
  *
  * Mode is decided by `spark.master` (spec §7): a `local[*]` master reads
  * `localproperties.xml` (files only, no HDFS/Postgres); any other master reads
  * `properties.xml`. `ConfigLoader` fails fast if the file's contents disagree with
  * the mode, so a local run can never silently reach HDFS/Postgres.
  */
object OrderProcessingApp {

  def main(args: Array[String]): Unit = {
    // Default to local[*] for `sbt run`. Under spark-submit the master is already set
    // (via SparkConf), so we only fill it in when absent — never override the cluster.
    val conf = new SparkConf().setAppName("OrderProcessing")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // Single shared SparkContext handle (spec §2): used for RDD ops and broadcasts,
    // passed implicitly to the utilities and pipeline stages.
    implicit val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    try {
      val cfg = ConfigLoader.load()
      println(s"[App] mode=${if (cfg.isLocal) "LOCAL" else "PROD"} " +
        s"bronze=${cfg.bronzePath} silver=${cfg.silverPath} gold=${cfg.goldPath}")

      // Local only: bootstrap sample inputs if they are not already present.
      if (cfg.isLocal) SampleDataGenerator.generateIfMissing(cfg)

      BronzeIngestion.run(cfg)
      SilverProcessing.run(cfg)
      GoldProcessing.run(cfg)

      println("[App] pipeline completed: Bronze -> Silver -> Gold")
    } finally {
      spark.stop()
    }
  }
}
