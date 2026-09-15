package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

import org.apache.spark.sql.{DataFrame, SaveMode}

/** Shared Spark write helper used by Bronze/Silver/Gold.
  *
  * All persistence goes through Spark's DataFrame writer (spec §5 I/O constraint) —
  * never the Hadoop FileSystem API.
  */
object SparkIO {

  /** Overwrite `path` with `df` as Parquet, logging the row count. */
  def writeParquet(df: DataFrame, path: String) {
    println(s"[SparkIO] writing ${df.count} rows to $path")
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }
}
