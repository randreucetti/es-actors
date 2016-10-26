name := "es-server"

version := "1.2.1"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.11"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.11" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.4.11"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "2.4.1"