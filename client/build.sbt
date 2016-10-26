name := "es-client"

version := "1.2.0"

scalaVersion := "2.11.8"

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.broilogabriel"
  )

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.11"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.11" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.4.11"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.5"

libraryDependencies += "joda-time" % "joda-time" % "2.9.4"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"