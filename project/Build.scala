
import sbt._
import sbt.Default._
import sbt.Keys._

import scalariform.formatter.preferences._
import com.typesafe.sbtscalariform.ScalariformPlugin._

object ApplicationBuild extends Build {

    lazy val projectScalariformSettings = defaultScalariformSettings ++ Seq(
        ScalariformKeys.preferences := FormattingPreferences()
            .setPreference(AlignParameters, true)
            .setPreference(FormatXml, false)
    )

    lazy val commonSettings: Seq[Setting[_]] = Project.defaultSettings ++ projectScalariformSettings ++ Seq(
        organization := "aws",
        scalaVersion := "2.10.0-RC1",
        scalacOptions += "-feature",
        version := "0.1-SNAPSHOT",
        resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases",
        resolvers += "Caffeine Lab" at "http://caffeinelab.net/repo",
        resolvers ++= Seq("sonatype" at "http://oss.sonatype.org/content/repositories/releases"),
        libraryDependencies += "play" %% "play" % "2.1-20121029-UP",
        libraryDependencies += "org.specs2" % "specs2_2.10.0-RC1" % "1.12.2" % "test",
        libraryDependencies += "com.novocode" % "junit-interface" % "0.10-M2",
        testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
    )

    lazy val core = Project("core", file("core"), settings = commonSettings)

    lazy val s3 = Project("s3", file("s3"), settings = commonSettings).dependsOn(core)

    lazy val sqs = Project("sqs", file("sqs"), settings = commonSettings).dependsOn(core)

    lazy val sns = Project("sns", file("sns"), settings = commonSettings).dependsOn(core)

    lazy val dynamodb = Project("dynamodb", file("dynamodb"), settings = commonSettings).dependsOn(core)

    lazy val simpledb = Project("simpledb", file("simpledb"), settings = commonSettings).dependsOn(core)

    lazy val ses = Project("ses", file("ses"), settings = commonSettings).dependsOn(core)

    lazy val root = Project("root", file("."), settings = Project.defaultSettings ++ Unidoc.settings).aggregate(
        core, s3, simpledb, sqs, sns, dynamodb, ses
    )

}

