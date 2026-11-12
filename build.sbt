ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "spark.replay"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint:unused")

// One build per Spark version: the engine and the capture jar use private[spark] classes.
val sparkVersion = "3.5.8"
val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided

// What Spark 3.5 needs on Java 17 (org.apache.spark.launcher.JavaModuleOptions).
val sparkJavaOptions = Seq(
  "-Xmx4g", "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED", "-Djdk.reflect.useDirectMethodHandle=false")

// Event classes, subplan keys and the skeleton codec: shared by the writer and the reader.
lazy val common = project.settings(libraryDependencies += sparkSql)

// What ships in the application image, together with `common`.
lazy val capture = project.dependsOn(common).settings(libraryDependencies += sparkSql)

// Event log -> the scheduling timeline of a run.
lazy val distiller = project.settings(libraryDependencies += sparkSql)

lazy val engine = project.dependsOn(common, distiller).settings(libraryDependencies += sparkSql)

lazy val fidelity = project.dependsOn(engine, capture).settings(
  libraryDependencies ++= Seq(sparkSql, "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    // Same Iceberg as the spark-base image; only the production replicas use it.
    "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.10.0" % Test),
  Test / fork := true,
  // One SparkContext per JVM.
  Test / parallelExecution := false,
  Test / javaOptions ++= sparkJavaOptions,
  publish / skip := true)
