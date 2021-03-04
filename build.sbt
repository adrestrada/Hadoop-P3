name := "course3project"

version := "0.1"

scalaVersion := "2.11.8"

idePackagePrefix := Some("ca.mcit.bigdata.hdfs")

val HadoopVersion = "2.7.7"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion
)