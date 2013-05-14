
import sbt._
import sbt.Default._
import sbt.Keys._

import scalariform.formatter.preferences._
import com.typesafe.sbtscalariform.ScalariformPlugin._

object AWS {
    val scalaVersion = "2.10.0"
    val version = "0.3-SNAPSHOT"
    val playVersion = "2.1.0"
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
        libraryDependencies ++= Seq(
          "play" %% "play" % AWS.playVersion,
          "org.specs2" %% "specs2" % "1.12.3" % "test",
          "com.novocode" % "junit-interface" % "0.10-M2" % "test",
          "commons-codec" % "commons-codec" % "1.7"
        ),
        testOptions += Tests.Argument(TestFrameworks.JUnit, "-v"),
        publishMavenStyle := true,
        publishTo <<= version { (version: String) =>
          val localPublishRepo = "../datomisca-repo/"
          if(version.trim.endsWith("SNAPSHOT"))
            Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
          else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
        }
        //testListeners <<= (target, streams).map((t, s) => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath, s.log)))
    )

    lazy val core = Project("core", file("core"), settings = commonSettings)

    //REVIST THIS AND MAKE IT WORK WITH NEW WAY OF DOING CREDENTIALS
    lazy val s3 = Project("s3", file("s3"), settings = commonSettings).dependsOn(core)

    //REVIST THIS AND MAKE IT WORK WITH NEW WAY OF DOING CREDENTIALS
    lazy val sqs = Project("sqs", file("sqs"), settings = commonSettings).dependsOn(core)

    lazy val sns = Project("sns", file("sns"), settings = commonSettings).dependsOn(core)

    lazy val dynamodb = Project("dynamodb", file("dynamodb"), settings = commonSettings).dependsOn(core)

    lazy val simpledb = Project("simpledb", file("simpledb"), settings = commonSettings).dependsOn(core)

    //REVIST THIS AND MAKE IT WORK WITH NEW WAY OF DOING CREDENTIALS
    //lazy val ses = Project("ses", file("ses"), settings = commonSettings).dependsOn(core)

    //REVIST THIS AND MAKE IT WORK WITH NEW WAY OF DOING CREDENTIALS
    //lazy val cloudsearch = Project("cloud-search", file("cloudsearch"), settings = commonSettings).dependsOn(core)

    lazy val wrap = Project("wrap", file("wrap"), settings = commonSettings ++ Seq(
        libraryDependencies ++= Seq(
            "com.amazonaws" % "aws-java-sdk" % "1.4.3",
            "ch.qos.logback" % "logback-classic" % "1.0.1"
        )
    ))

    lazy val root = Project("root", file("."), settings = Project.defaultSettings ++ Unidoc.settings).aggregate(
        core, simpledb, sns, dynamodb, s3, sqs
    )

}

