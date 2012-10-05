
import sbt._
import sbt.Default._
import sbt.Keys._

import scalariform.formatter.preferences._
import com.typesafe.sbtscalariform.ScalariformPlugin._

object ApplicationBuild extends Build {

    lazy val projectScalariformSettings = defaultScalariformSettings ++ Seq(
        ScalariformKeys.preferences := FormattingPreferences().setPreference(AlignParameters, true)
    )

    lazy val commonSettings: Seq[Setting[_]] = Project.defaultSettings ++ projectScalariformSettings ++ Seq(
        organization := "pellucid",
        scalaVersion := "2.9.2",
        version := "0.1-SNAPSHOT",
        resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases",
        resolvers += "erwan" at "http://caffeinelab.net/repo",
        libraryDependencies += "playlibs" %% "ws" % "2.1-20121005",
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

