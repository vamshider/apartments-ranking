name := "BigDataProject"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.4.3"

lazy val root = (project in file(".")).
  settings(
    name := "BigDataProject",
    version := "1.0",
    scalaVersion := "2.11.8"
  )

resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"


libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "2.3.0",
  "com.microsoft.azure" % "azure-sqldb-spark" % "1.0.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0",
  "databricks" % "spark-corenlp" % "0.2.0-s_2.11",
  "org.apache.spark" %% "spark-sql" % "2.4.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

scalaSource in Compile := baseDirectory.value / "src"


