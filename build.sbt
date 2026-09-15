// =============================================================================
// spark-migration-example — root build (sbt multi-module)
// -----------------------------------------------------------------------------
// A legacy Scala/Spark project targeting Spark 2.4 / Scala 2.11 / Java 8.
//
// Two independent modules, each producing its OWN thin jar:
//   - common          -> scala-migration-common_2.11-<ver>.jar        (utilities)
//   - orderProcessing -> scala-migration-orderprocessing_2.11-<ver>.jar (pipeline + main)
//
// orderProcessing dependsOn common, so `common` is a compile- AND run-time
// dependency of orderProcessing — never the reverse. Thin jars are used
// (plain `package`), so at runtime BOTH jars go on the classpath
// (spark-submit --jars <common.jar>). `common` is not fat-jarred into the app.
// =============================================================================

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.databricks.migrationgenie"

// ---- Shared dependency coordinates -----------------------------------------
// NOTE ON SCOPE: spark-core / spark-sql are `compile` here so the app runs
// locally via `sbt orderProcessing/run`. For a real cluster deploy, change them
// to `% Provided` (the cluster supplies Spark) and rebuild the thin jars.
val sparkVersion = "2.4.8"

val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
val sparkSql  = "org.apache.spark" %% "spark-sql"  % sparkVersion
val postgres  = "org.postgresql"    % "postgresql" % "42.2.27"
val scalatest = "org.scalatest"    %% "scalatest"  % "3.0.8"

// Spark 2.4 forks a JVM; fork tests so Spark's local session behaves.
ThisBuild / Test / fork := true
ThisBuild / Test / parallelExecution := false

// ---- MODULE 1: common (no dependency on any other module) ------------------
lazy val common = (project in file("common"))
  .settings(
    name := "scala-migration-common",
    libraryDependencies ++= Seq(
      sparkCore,          // RDD API + SparkContext (ConfigLoader, DataQualityChecks)
      sparkSql,           // DataFrame/JDBC (PostgresReader) + SparkSession (ConfigLoader)
      postgres,           // JDBC driver used by PostgresReader
      scalatest % Test
    )
  )

// ---- MODULE 2: orderProcessing (dependsOn common) --------------------------
lazy val orderProcessing = (project in file("orderProcessing"))
  .dependsOn(common) // <-- one-directional dependency: common is a lib of orderProcessing
  .settings(
    name := "scala-migration-orderprocessing",
    libraryDependencies ++= Seq(
      sparkCore,
      sparkSql,
      scalatest % Test
    ),
    // Local run entry point.
    Compile / mainClass := Some(
      "com.databricks.migrationgenie.scalamigrationexample.orderprocessing.OrderProcessingApp"
    ),
    // `sbt orderProcessing/run` needs a forked JVM for Spark's local mode.
    fork := true
  )

// Aggregate so `sbt compile` / `sbt test` cover both modules.
lazy val root = (project in file("."))
  .aggregate(common, orderProcessing)
  .settings(
    name := "spark-migration-example",
    // Nothing to publish/run at the root itself.
    publish / skip := true
  )
