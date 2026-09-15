package com.databricks.migrationgenie.scalamigrationexample.commonutilities

import java.util.Properties

import org.apache.spark.sql.SparkSession

/** Postgres CONNECTION config (prod only). The SQL queries are code, not config —
  * see `BronzeIngestion.ProductQuery` / `RegionQuery`. */
final case class PgConfig(
  url: String,
  user: String,
  password: String,
  driver: String
)

/** Typed view of the application configuration.
  *
  * All `*Path` fields are ALREADY scheme-qualified by [[PathResolver]] (i.e. they
  * start with `file://` in local mode or `hdfs://` in prod). The scheme is added in
  * code — it is never present in the property files. `productPath` / `regionPath`
  * are populated only for file sources (empty string when the source is `jdbc`).
  */
final case class AppConfig(
  isLocal: Boolean,
  orderPath: String,        // fully-qualified (scheme added in code)
  orderDetailsPath: String, // fully-qualified
  productPath: String,      // fully-qualified, or "" when productSource == "jdbc"
  regionPath: String,       // fully-qualified, or "" when regionSource == "jdbc"
  productSource: String,    // "file" | "jdbc"
  regionSource: String,     // "file" | "jdbc"
  bronzePath: String,       // fully-qualified
  silverPath: String,       // fully-qualified
  goldPath: String,         // fully-qualified
  postgres: Option[PgConfig]
)

/** Loads the XML property file that matches the run mode, resolves bare paths to
  * scheme-qualified URIs, and validates that the file's CONTENTS agree with the mode.
  *
  * Mode is derived from `spark.master`:
  *   - `local`, `local[*]`, `local[4]` … => local mode  -> `localproperties.xml`
  *   - anything else                      => prod  mode  -> `properties.xml`
  */
object ConfigLoader {

  private val LocalResource = "/localproperties.xml"
  private val ProdResource  = "/properties.xml"

  /** Load + validate config for the active SparkSession. Fails fast on any mismatch. */
  def load()(implicit spark: SparkSession): AppConfig = {
    val master = spark.sparkContext.master
    // startsWith("local") matches local, local[*], local[4], local[*,2], … — a bare
    // `== "local"` would miss the common `local[*]` form and mis-route the config.
    val isLocal = master.startsWith("local")

    val resourceName = if (isLocal) LocalResource else ProdResource
    val props = loadProps(resourceName)

    // --- small accessors ------------------------------------------------------
    def required(key: String): String = {
      val v = props.getProperty(key)
      require(v != null, s"Missing required property '$key' in $resourceName")
      v.trim
    }

    val productSource = required("data.product.source")
    val regionSource  = required("data.region.source")

    // --- no scheme allowed in any path property (scheme belongs in code) -------
    val pathKeys =
      Seq("data.order.path", "data.orderDetails.path",
          "output.bronze.path", "output.silver.path", "output.gold.path") ++
        (if (productSource == "file") Seq("data.product.path") else Nil) ++
        (if (regionSource == "file") Seq("data.region.path") else Nil)
    pathKeys.foreach { k =>
      val v = required(k)
      require(!v.contains("://"),
        s"Property '$k' must be a BARE path (no scheme) — the scheme is added in code. Got: $v")
    }

    // --- Postgres block only when a jdbc source is actually declared (prod) ----
    val postgres: Option[PgConfig] =
      if (!isLocal && (productSource == "jdbc" || regionSource == "jdbc")) {
        Some(PgConfig(
          url      = required("postgres.url"),
          user     = required("postgres.user"),
          password = required("postgres.password"),
          driver   = required("postgres.driver")
        ))
      } else None

    // --- resolve bare paths -> scheme-qualified URIs (scheme chosen by mode) ---
    def resolved(key: String): String = PathResolver.resolve(required(key), isLocal)

    val cfg = AppConfig(
      isLocal          = isLocal,
      orderPath        = resolved("data.order.path"),
      orderDetailsPath = resolved("data.orderDetails.path"),
      productPath      = if (productSource == "file") resolved("data.product.path") else "",
      regionPath       = if (regionSource == "file") resolved("data.region.path") else "",
      productSource    = productSource,
      regionSource     = regionSource,
      bronzePath       = resolved("output.bronze.path"),
      silverPath       = resolved("output.silver.path"),
      goldPath         = resolved("output.gold.path"),
      postgres         = postgres
    )

    validate(cfg, master)
    cfg
  }

  /** Belt-and-suspenders check (spec §5): fail fast BEFORE any Spark I/O so a
    * misconfigured mode can never silently reach HDFS/Postgres during a local run.
    * The property file is chosen by the master, so this guards a hand-edited file
    * whose contents disagree with the mode (e.g. jdbc sources in localproperties.xml).
    */
  private def validate(cfg: AppConfig, master: String): Unit = {
    if (cfg.isLocal) {
      require(
        cfg.productSource == "file" && cfg.regionSource == "file" && cfg.postgres.isEmpty,
        s"local master ($master) requires file sources and no Postgres, got " +
          s"product=${cfg.productSource}, region=${cfg.regionSource}, postgres=${cfg.postgres.isDefined}"
      )
    } else {
      require(
        !(cfg.productSource == "jdbc" || cfg.regionSource == "jdbc") || cfg.postgres.isDefined,
        "prod jdbc source requires a Postgres configuration block"
      )
    }
  }

  /** Read a Java XML-format properties resource from the classpath. */
  private def loadProps(resourceName: String): Properties = {
    val is = getClass.getResourceAsStream(resourceName)
    require(is != null, s"Config resource not found on classpath: $resourceName")
    try {
      val p = new Properties()
      p.loadFromXML(is) // Java XML properties format (<properties><entry key=..>..)
      p
    } finally {
      is.close()
    }
  }
}
