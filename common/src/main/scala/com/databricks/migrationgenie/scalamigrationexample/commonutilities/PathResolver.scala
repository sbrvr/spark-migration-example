package com.databricks.migrationgenie.scalamigrationexample.commonutilities

/** Composes fully-qualified filesystem URIs from the BARE paths that live in the
  * property files.
  *
  * DESIGN RULE (see spec §7): property files contain bare paths only — no scheme,
  * no authority (`/data/raw/orders`, never `hdfs:///data/raw/orders`). The scheme
  * is defined HERE, in code, and chosen by run mode:
  *   - local mode -> `file://`
  *   - prod  mode -> `hdfs://`  (empty authority => cluster's default `fs.defaultFS`)
  *
  * Keeping the scheme in code (not config) means a local test run can never be
  * pointed at HDFS by an edited property file.
  */
object PathResolver {

  /** Scheme prepended in LOCAL mode. Hardcoded here, never read from a property. */
  val LocalScheme = "file://"

  /** Scheme prepended in PROD mode. Hardcoded here, never read from a property. */
  val HdfsScheme = "hdfs://"

  /** Prepend the current mode's scheme to a bare path.
    *
    * Example: `resolve("/data/raw/orders", isLocal = false)` => `"hdfs:///data/raw/orders"`
    *          `resolve("/tmp/x/orders.csv", isLocal = true)`  => `"file:///tmp/x/orders.csv"`
    *
    * @param barePath a path with no scheme/authority (as stored in a property file)
    * @param isLocal  true for a `local[*]` master, false otherwise
    * @return the scheme-qualified URI string
    */
  def resolve(barePath: String, isLocal: Boolean): String = {
    // Guard: this helper exists precisely so schemes never come from config. If a
    // scheme slipped through, fail loudly rather than produce `hdfs://file:///...`.
    require(!barePath.contains("://"),
      s"PathResolver expects a bare path (no scheme), got: $barePath")
    (if (isLocal) LocalScheme else HdfsScheme) + barePath
  }
}
