package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import org.scalatest.FunSuite

/** Unit tests for [[PathResolver]] — no Spark needed. */
class PathResolverSpec extends FunSuite {

  test("local mode prepends file://") {
    assert(PathResolver.resolve("/tmp/sparktestdata/orders.csv", isLocal = true) ==
      "file:///tmp/sparktestdata/orders.csv")
  }

  test("prod mode prepends hdfs:// (empty authority => default namenode)") {
    assert(PathResolver.resolve("/data/raw/orders", isLocal = false) ==
      "hdfs:///data/raw/orders")
  }

  test("rejects a path that already contains a scheme") {
    intercept[IllegalArgumentException] {
      PathResolver.resolve("hdfs:///data/raw/orders", isLocal = false)
    }
  }
}
