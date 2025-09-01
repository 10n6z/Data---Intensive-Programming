name := "dip24-ex4"
version := "1.0"
scalaVersion := "2.12.20"

val SparkVersion: String = "3.5.0"
val NsplVersion: String = "0.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % SparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion
// for the plotting functionality
libraryDependencies += "io.github.pityka" %% "nspl-awt" % NsplVersion

// suppress all log messages for setting up the Spark Session
javaOptions += "-Dlog4j.configurationFile=project/log4j.properties"

// to avoid java.nio.file.NoSuchFileException at the end of execution
run / fork := true
