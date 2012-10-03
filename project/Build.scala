
import sbt._
import sbt.Default._
import sbt.Keys._

object ApplicationBuild extends Build {

    lazy val commonSettings: Seq[Setting[_]] = Project.defaultSettings ++ Seq(
        organization := "pellucid",
        scalaVersion := "2.9.1",
        version := "0.1-SNAPSHOT",
        resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases",
        libraryDependencies += "play" %% "play" % "2.0.4"
    )

    lazy val core = Project("core", file("core"), settings = commonSettings)

    lazy val s3 = Project("s3", file("s3"), settings = commonSettings).dependsOn(
        core
    )

    lazy val root = Project("root", file(".")).aggregate(
        core, s3
    )

}

