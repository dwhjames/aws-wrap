
import sbt._
import sbt.Default._
import sbt.Keys._

import scalariform.formatter.preferences._
import com.typesafe.sbtscalariform.ScalariformPlugin._

object AWS {
    val scalaVersion = "2.10.0"
    val version = "0.4-SNAPSHOT"
    val repository = "AWS" at "http://pellucidanalytics.github.com/aws/repository/"

}

object ApplicationBuild extends Build {

    lazy val projectScalariformSettings = defaultScalariformSettings ++ Seq(
        ScalariformKeys.preferences := FormattingPreferences()
            .setPreference(AlignParameters, true)
            .setPreference(FormatXml, false)
    )

    lazy val commonSettings: Seq[Setting[_]] = Project.defaultSettings ++ projectScalariformSettings ++ Seq(
        organization := "aws",
        scalaVersion := AWS.scalaVersion,
        scalacOptions ++= Seq("-feature", "-deprecation"),
        version := AWS.version,
        resolvers ++= Seq(
          "typesafe" at "http://repo.typesafe.com/typesafe/releases",
          "sonatype" at "http://oss.sonatype.org/content/repositories/releases"
        ),
        publishMavenStyle := true,
        publishTo <<= version { (version: String) =>
          val localPublishRepo = "../datomisca-repo/"
          if(version.trim.endsWith("SNAPSHOT"))
            Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
          else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
        }
    )

    lazy val wrap = Project("wrap", file("wrap"), settings = commonSettings ++ Seq(
        libraryDependencies ++= Seq(
            "com.amazonaws" % "aws-java-sdk" % "1.4.3",
            "ch.qos.logback" % "logback-classic" % "1.0.1"
        )
    ))

}

