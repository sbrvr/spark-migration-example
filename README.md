# spark-migration-example

A **legacy** Scala/Spark order-processing app (Spark 2.4 · Scala 2.11 · Java 8 · sbt).
It follows the medallion pattern (Bronze → Silver → Gold) over Parquet, is RDD-first
(DataFrames only for Parquet/JDBC I/O and the Gold joins).

## Modules (two jars)

| Module | Jar | Contents | Depends on |
|--------|-----|----------|------------|
| `common` | `scala-migration-common_2.11-0.1.0.jar` | FileChecker, PathResolver, PostgresReader, DataQualityChecks, ConfigLoader | — |
| `orderProcessing` | `scala-migration-orderprocessing_2.11-0.1.0.jar` | Models, SampleDataGenerator, Bronze/Silver/Gold, `OrderProcessingApp` | `common` |

## Prerequisites

- **JDK 8** (Spark 2.4 requires it). Point `JAVA_HOME` at a Java 8 JDK, e.g.:
  ```bash
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home
  ```
- **sbt** — the build pins sbt `1.9.9` (via `project/build.properties`); the launcher honors it.



## Build

```bash
# both thin jars
sbt common/package orderProcessing/package
# -> common/target/scala-2.11/scala-migration-common_2.11-0.1.0.jar
# -> orderProcessing/target/scala-2.11/scala-migration-orderprocessing_2.11-0.1.0.jar
```

## Run locally

```bash
sbt orderProcessing/run
```

- Uses `spark.master=local[*]` → reads `localproperties.xml` → **files only** (never HDFS/Postgres).
- Auto-generates sample CSVs under `/tmp/sparktestdata/` if absent.
- Writes Parquet to `/tmp/sparktestdata/output/{bronze,silver,gold}`.

## Test

```bash
sbt test                 # both modules
sbt common/test          # unit tests (FileChecker, PathResolver, DataQualityChecks)
sbt orderProcessing/test # end-to-end local pipeline
```

## Deploy to a cluster (reference)

Switch the Spark deps to `% Provided` in `build.sbt`, rebuild, then:

```bash
spark-submit --master yarn \
  --class com.databricks.migrationgenie.scalamigrationexample.orderprocessing.OrderProcessingApp \
  --jars scala-migration-common_2.11-0.1.0.jar,postgresql-42.2.27.jar \
  scala-migration-orderprocessing_2.11-0.1.0.jar
```

Prod mode reads `properties.xml`: order files from HDFS (`hdfs://` added in code), product/region
from Postgres. Paths in the property files are **bare** — the `hdfs://` / `file://` scheme is
prepended by `PathResolver`.
