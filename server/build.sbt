name := "es-server"

version := "1.3.5"

scalaVersion := "2.11.8"

showSuccess := false

logLevel in run := Level.Warn

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.broilogabriel"
  )

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.11"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.11" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.4.11"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "2.4.1"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"

