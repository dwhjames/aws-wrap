
import sbt._
import sbt.Default._
import sbt.Keys._

object ApplicationBuild extends Build {

    lazy val commonSettings: Seq[Setting[_]] = Project.defaultSettings ++ Seq(
        organization := "pellucid",
        scalaVersion := "2.9.2",
        version := "0.1-SNAPSHOT",
        resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases",
        resolvers += "erwan" at "http://caffeinelab.net/repo",
        libraryDependencies += "play" %% "play" % "2.1-20121003-aws",
        libraryDependencies += "org.specs2" %% "specs2" % "1.11" % "test"
    )

    lazy val core = Project("core", file("core"), settings = commonSettings)

    lazy val s3 = Project("s3", file("s3"), settings = commonSettings).dependsOn(core)

    lazy val dynamodb = Project("dynamodb", file("dynamodb"), settings = commonSettings).dependsOn(core)

    lazy val simpledb = Project("simpledb", file("simpledb"), settings = commonSettings).dependsOn(core)

    lazy val root = Project("root", file(".")).aggregate(
        core, s3, simpledb, dynamodb
    )

}

