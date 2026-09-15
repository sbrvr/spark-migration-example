package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** Verifies FileChecker.exists via `file://` URIs (local filesystem). */
class FileCheckerSpec extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private implicit var sc: SparkContext = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("FileCheckerSpec").getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("exists returns true for a real local file") {
    val f = File.createTempFile("filechecker", ".txt")
    f.deleteOnExit()
    assert(FileChecker.exists("file://" + f.getAbsolutePath))
  }

  test("exists returns false for a missing path") {
    assert(!FileChecker.exists(s"file:///no/such/path_${System.nanoTime()}"))
  }

  test("assertExists throws for a missing path") {
    intercept[IllegalStateException] {
      FileChecker.assertExists(s"file:///no/such/path_${System.nanoTime()}")
    }
  }
}
