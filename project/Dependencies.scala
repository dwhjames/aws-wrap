import sbt._

object Dependencies {

  object V {
    val awsJavaSDK  = "1.8.4"

    val jodaTime    = "2.3"
    val jodaConvert = "1.6"

    val slf4j = "1.7.7"

    val logback     = "1.1.1"
  }

  object Compile {

    val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % V.awsJavaSDK exclude("joda-time", "joda-time")

    val jodaTime    = "joda-time" % "joda-time"    % V.jodaTime
    val jodaConvert = "org.joda"  % "joda-convert" % V.jodaConvert

    val slf4j = "org.slf4j" % "slf4j-api" % V.slf4j

    val logback    = "ch.qos.logback" % "logback-classic" % V.logback
  }

  object IntegrationTest {

    val scalaTest = "org.scalatest" % "scalatest_2.10" % "1.9.2" % "it"
  }
}
