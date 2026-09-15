// No plugins are required for the default build (thin jars via `package`).
//
// If you ever need a single fat jar instead of the thin-jar + `--jars` model,
// uncomment sbt-assembly below and run `orderProcessing/assembly`. Note this
// would bundle `common` INTO the app jar, which is the opposite of the
// two-separate-jars design this project intends — use it only intentionally.
//
// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
