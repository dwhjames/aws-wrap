
import sbt._
import sbt.Keys._
import sbt.Project.Initialize

object AWSBuild extends Build {

  lazy val buildSettings = Seq(
    organization := "aws",
    version      := "0.4-SNAPSHOT",
    scalaVersion := "2.10.1",
    scalacOptions ++= Seq("-feature", "-deprecation")
  )

  override lazy val settings =
    super.settings ++
    buildSettings ++
    Seq(
      shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
    )

  lazy val wrap = Project(
    id       = "wrap",
    base     = file("wrap"),
    settings =
      Defaults.defaultSettings ++
      Publish.settings ++
      Seq(
        resolvers ++= Seq(
          "typesafe" at "http://repo.typesafe.com/typesafe/releases",
          "sonatype" at "http://oss.sonatype.org/content/repositories/releases"
        ),
        libraryDependencies ++= Dependencies.wrap
      )
  )

}

object Dependencies {

  object Compile {

    val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % "1.4.3"

    val jodaTime    = "joda-time" % "joda-time"    % "2.2"
    val jodaConvert = "org.joda"  % "joda-convert" % "1.3.1"

    val logback    = "ch.qos.logback" % "logback-classic" % "1.0.1"
  }

  import Compile._

  val wrap = Seq(awsJavaSDK, jodaTime, jodaConvert, logback)

}

object Publish {

  lazy val settings = Seq(
    publishMavenStyle := true,
    publishTo <<= localPublishTo
  )

  // "AWS" at "http://pellucidanalytics.github.com/aws/repository/"
  def localPublishTo: Initialize[Option[Resolver]] = {
    version { v: String =>
      val localPublishRepo = "../datomisca-repo/"
      if (v.trim endsWith "SNAPSHOT")
        Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
      else
        Some(Resolver.file("releases",  new File(localPublishRepo + "/releases")))
    }
  }

}
