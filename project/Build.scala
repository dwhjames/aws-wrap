
import sbt._
import sbt.Default._
import sbt.Keys._

import scalariform.formatter.preferences._
import com.typesafe.sbtscalariform.ScalariformPlugin._

object ApplicationBuild extends Build {

    val pattern = Patterns(
      Seq("[organisation]/[module]/[revision]/[module]-[revision](-[classifier]).ivy"),
      Seq("[organisation]/[module]/[revision]/[module]-[revision](-[classifier]).[ext]"),
      true
    )

    lazy val projectScalariformSettings = defaultScalariformSettings ++ Seq(
        ScalariformKeys.preferences := FormattingPreferences()
            .setPreference(AlignParameters, true)
            .setPreference(FormatXml, false)
    )

    lazy val commonSettings: Seq[Setting[_]] = Project.defaultSettings ++ projectScalariformSettings ++ Seq(
        organization := "aws",
        scalaVersion := "2.10.0-M7",
        scalacOptions += "-feature",
        version := "0.1-SNAPSHOT",
        resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases",
        resolvers += "erwan" at "http://caffeinelab.net/repo",
        resolvers ++= Seq("sonatype" at "http://oss.sonatype.org/content/repositories/releases"),
        libraryDependencies += "play" %% "play" % "2.1-20121017-erw",
        libraryDependencies += "org.specs2" % "specs2_2.10.0-M7" % "1.12.1.1" % "test"
    )

    lazy val core = Project("core", file("core"), settings = commonSettings)

    lazy val s3 = Project("s3", file("s3"), settings = commonSettings).dependsOn(core)

    lazy val sns = Project("sns", file("sns"), settings = commonSettings).dependsOn(core)

    lazy val dynamodb = Project("dynamodb", file("dynamodb"), settings = commonSettings).dependsOn(core)

    lazy val simpledb = Project("simpledb", file("simpledb"), settings = commonSettings).dependsOn(core)

    lazy val root = Project("root", file("."), settings = Project.defaultSettings ++ Unidoc.settings).aggregate(
        core, s3, simpledb, sns, dynamodb
    )

}

