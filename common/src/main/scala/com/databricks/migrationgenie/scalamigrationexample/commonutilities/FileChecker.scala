package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/** Existence checks for input paths.
  *
  * I/O CONSTRAINT (spec §5): this is the ONLY place in the codebase that touches the
  * Hadoop `FileSystem` API, and it is used ONLY to test existence — never to open,
  * create, list, delete, or rename. All actual data movement goes through Spark
  * (`spark.read.*` / `df.write.*`).
  *
  * The `FileSystem` is resolved from the path's URI, so the same code works for
  * `file://…` (local/tests) and `hdfs://…` (prod) with no branching here.
  */
object FileChecker {

  /** @return true iff `path` exists on the filesystem implied by its URI scheme. */
  def exists(path: String)(implicit sc: SparkContext): Boolean = {
    val uri = new URI(path)
    // FileSystem.get(uri, conf) picks the concrete FS (local vs hdfs) from the scheme.
    val fs: FileSystem = FileSystem.get(uri, sc.hadoopConfiguration)
    fs.exists(new Path(path))
  }

  /** Throw a clear error if `path` is missing. Used by Bronze for the order files. */
  def assertExists(path: String)(implicit sc: SparkContext): Unit = {
    if (!exists(path)) {
      throw new IllegalStateException(s"Required input path does not exist: $path")
    }
  }
}
