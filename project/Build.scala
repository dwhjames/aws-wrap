
import scala.language.postfixOps

import sbt._
import sbt.Keys._


object AWSWrapBuild extends Build {

  lazy val buildSettings = Seq(
    organization := "com.pellucid",
    version      := "0.5-RC5",
    scalaVersion := "2.10.3",
    scalacOptions ++= Seq("-feature", "-deprecation", "-unchecked"),
    shellPrompt  := CustomShellPrompt.customPrompt
  )

  override lazy val settings =
    super.settings ++
    buildSettings

  lazy val awsWrap = Project(
    id       = "aws-wrap",
    base     = file(".")
  ) settings (awsWrapSettings:_*)

  lazy val awsWrapTest = Project(
    id       = "aws-wrap-test",
    base     = file("integration")
  ) dependsOn (awsWrap) configs (IntegrationTest) settings (awsWrapTestSettings:_*)

  lazy val scratch = Project(
    id = "scratch",
    base = file("scratch")
  ) dependsOn (awsWrap) settings(scratchSettings:_*)

  lazy val commonSettings =
    Defaults.defaultSettings ++
    Seq(
      resolvers ++= Seq(
        "typesafe" at "http://repo.typesafe.com/typesafe/releases",
        "sonatype" at "http://oss.sonatype.org/content/repositories/releases"
      )
    )

  lazy val awsWrapSettings =
    commonSettings ++
    bintray.Plugin.bintraySettings ++
    Seq(
      libraryDependencies ++= Dependencies.awsWrap,

      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
      bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("pellucid"),
      bintray.Keys.packageLabels in bintray.Keys.bintray := Seq("aws", "dynamodb", "s3", "ses", "simpledb", "sns", "sqs", "async", "future")
    )

  val localMode = settingKey[Boolean]("Run integration tests locally")

  lazy val awsWrapTestSettings =
    commonSettings ++
    Defaults.itSettings ++
    Seq(
      libraryDependencies ++= Dependencies.awsWrapTest,

      localMode := true,
      parallelExecution in IntegrationTest := false,

      // testOptions in IntegrationTest += Tests.Argument(s"-D=${localMode.value}")

      testOptions in IntegrationTest += Tests.Setup { () =>
        if (localMode.value) {
          println("Start DynamoDB Local")
          System.setProperty("DynamoDB.localMode", "true")
          Process("bash start-dynamodb-local.sh") !
        }
      },

      testOptions in IntegrationTest += Tests.Cleanup { () =>
        if (localMode.value) {
          println("Stop DynamoDB Local")
          System.clearProperty("DynamoDB.localMode")
          Process("bash stop-dynamodb-local.sh") !
        }
      }
    )

  lazy val scratchSettings =
    commonSettings ++
    Seq(
      libraryDependencies ++= Dependencies.scratch,
      publish      := (),
      publishLocal := ()
    )

}

object Dependencies {

  object V {
    val awsJavaSDK  = "1.7.2"

    val jodaTime    = "2.3"
    val jodaConvert = "1.6"

    val slf4j = "1.7.6"

    val logback     = "1.1.1"
  }

  object Compile {

    val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % V.awsJavaSDK

    val jodaTime    = "joda-time" % "joda-time"    % V.jodaTime
    val jodaConvert = "org.joda"  % "joda-convert" % V.jodaConvert

    val slf4j = "org.slf4j" % "slf4j-api" % V.slf4j

    val logback    = "ch.qos.logback" % "logback-classic" % V.logback
  }

  object IntegrationTest {

    val scalaTest = "org.scalatest" % "scalatest_2.10" % "1.9.2" % "it"
  }

  import Compile._
  import IntegrationTest._

  val awsWrap = Seq(awsJavaSDK, slf4j)
  val awsWrapTest = Seq(awsJavaSDK, jodaTime, jodaConvert, scalaTest, logback)
  val scratch = Seq(awsJavaSDK, jodaTime, jodaConvert, logback)

}

object CustomShellPrompt {

  val Branch = """refs/heads/(.*)\s""".r

  def gitBranchOrSha =
    Process("git symbolic-ref HEAD") #|| Process("git rev-parse --short HEAD") !! match {
      case Branch(name) => name
      case sha          => sha.stripLineEnd
    }

  val customPrompt = { state: State =>

    val extracted = Project.extract(state)
    import extracted._

    (name in currentRef get structure.data) map { name =>
      "[" + scala.Console.CYAN + name + scala.Console.RESET + "] " +
      scala.Console.BLUE + "git:(" +
      scala.Console.RED + gitBranchOrSha +
      scala.Console.BLUE + ")" +
      scala.Console.RESET + " $ "
    } getOrElse ("> ")

  }
}
