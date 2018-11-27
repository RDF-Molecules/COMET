name := """play-scala-starter-example"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.11.12", "2.12.7")

libraryDependencies += guice
libraryDependencies += ehcache
libraryDependencies += cacheApi
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies += "com.h2database" % "h2" % "1.4.197"
libraryDependencies += "org.apache.jena" % "apache-jena-libs" % "3.0.1"
libraryDependencies += "com.h2database" % "h2" % "1.4.197"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
