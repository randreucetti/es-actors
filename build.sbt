name := "socketserver"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.11"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.11" % "test"

libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.8.3"

//libraryDependencies += "joda-time" % "joda-time" % "2.9.4"

//libraryDependencies += "org.joda" % "joda-convert" % "1.8.1"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.5"