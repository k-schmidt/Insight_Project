name := "ProcessTopics"

version := "1.0"

scalaVersion := "2.11.8"
sparkVersion := "2.1.1"
sparkComponents ++= Seq("core")

assemblyJarName in assembly := "ProcessTopics.jar"

mainClass in assembly := Some("com.github.kschmidt.ProcessTopics")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1",
  "mysql" % "mysql-connector-java" % "5.1.42"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}