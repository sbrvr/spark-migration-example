package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** Unit tests for the RDD-based data-quality helpers, using hand-built RDDs with
  * known violations. */
class DataQualityChecksSpec extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("DataQualityChecksSpec").getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("countNullKeys detects null and blank keys") {
    val rdd = sc.parallelize(Seq("a", "", "b", null.asInstanceOf[String], "  "))
    assert(DataQualityChecks.countNullKeys(rdd)(identity) == 3L) // "", null, "  "
  }

  test("countDuplicateKeys counts duplicated groups") {
    val rdd = sc.parallelize(Seq(1, 1, 2, 3, 3, 3))
    assert(DataQualityChecks.countDuplicateKeys(rdd)(identity) == 2L) // groups {1},{3}
  }

  test("dedupeByKey keeps one row per key") {
    val rdd = sc.parallelize(Seq((1, "x"), (1, "y"), (2, "z")))
    assert(DataQualityChecks.dedupeByKey(rdd)(_._1).count() == 2L)
  }

  test("missingReferences and dropOrphans use the broadcast parent set") {
    val parents = sc.broadcast(Set(1, 2, 3))
    val child = sc.parallelize(Seq(1, 2, 9, 4)) // 9 and 4 are orphans
    assert(DataQualityChecks.missingReferences(child, parents)(identity) == 2L)
    assert(DataQualityChecks.dropOrphans(child, parents)(identity).collect().toSet == Set(1, 2))
  }
}
