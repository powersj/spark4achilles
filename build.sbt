name := "cse8803_project"

version := "1.3"

scalaVersion := "2.10.6"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.6"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.6"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.6"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.10"


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}