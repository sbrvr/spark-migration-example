package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Reads Postgres tables/queries as DataFrames (prod product/region sources).
  *
  * JDBC is a DataFrame-only capability, so this crosses to the DataFrame API by
  * design (spec §2 boundary rule). Callers in Bronze immediately convert the
  * result back to an RDD (`.as[T].rdd`) to stay RDD-first for row-level work.
  *
  * @param pg Postgres connection + query settings (from [[PgConfig]]).
  */
class PostgresReader(pg: PgConfig)(implicit spark: SparkSession) {

  private def reader =
    spark.read
      .format("jdbc")
      .option("url", pg.url)
      .option("user", pg.user)
      .option("password", pg.password)
      .option("driver", pg.driver)

  /** Read a wrapped subquery, e.g. "(SELECT ... FROM product) AS p". */
  def readQuery(query: String): DataFrame =
    reader.option("dbtable", query).load()

  /** Read a plain table by name. */
  def readTable(table: String): DataFrame =
    reader.option("dbtable", table).load()
}
