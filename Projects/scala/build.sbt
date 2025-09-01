name := "dip24-assignment"
version := "1.0"
scalaVersion := "2.12.20"

Compile / run / mainClass := Some("dip24.assignment.Assignment")
Compile / scalacOptions += "-Xlint"

val SparkVersion: String = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion

// suppress all log messages for setting up the Spark Session
javaOptions += "-Dlog4j.configurationFile=project/log4j.properties"

// to avoid java.nio.file.NoSuchFileException at the end of execution
run / fork := true
