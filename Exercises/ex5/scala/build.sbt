name := "dip24-ex5"
version := "1.0"
scalaVersion := "2.12.20"

val SparkVersion: String = "3.5.0"
val DeltaVersion: String = "3.2.0"

libraryDependencies += "io.delta" %% "delta-spark" % DeltaVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion

// suppress all log messages for setting up the Spark Session
javaOptions += "-Dlog4j.configurationFile=project/log4j.properties"

// to avoid java.nio.file.NoSuchFileException at the end of execution
run / fork := true
