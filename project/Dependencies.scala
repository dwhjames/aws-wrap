import sbt._

object Dependencies {

  object V {
    val awsJavaSDK  = "1.10.32"

    val jodaTime    = "2.9"
    val jodaConvert = "1.8"

    val slf4j = "1.7.12"

    val logback     = "1.1.3"
  }

  object Compile {

    val awsJavaSDK_cloudwatch = "com.amazonaws" % "aws-java-sdk-cloudwatch" % V.awsJavaSDK
    val awsJavaSDK_dynamodb = "com.amazonaws" % "aws-java-sdk-dynamodb" % V.awsJavaSDK
    val awsJavaSDK_s3 = "com.amazonaws" % "aws-java-sdk-s3" % V.awsJavaSDK
    val awsJavaSDK_ses = "com.amazonaws" % "aws-java-sdk-ses" % V.awsJavaSDK
    val awsJavaSDK_simpledb = "com.amazonaws" % "aws-java-sdk-simpledb" % V.awsJavaSDK
    val awsJavaSDK_sns = "com.amazonaws" % "aws-java-sdk-sns" % V.awsJavaSDK
    val awsJavaSDK_sqs = "com.amazonaws" % "aws-java-sdk-sqs" % V.awsJavaSDK
    // val awsJavaSDK = "com.amazonaws" % "aws-java-sdk" % V.awsJavaSDK exclude("joda-time", "joda-time")

    val jodaTime    = "joda-time" % "joda-time"    % V.jodaTime
    val jodaConvert = "org.joda"  % "joda-convert" % V.jodaConvert

    val slf4j = "org.slf4j" % "slf4j-api" % V.slf4j

    val logback    = "ch.qos.logback" % "logback-classic" % V.logback
  }

  object IntegrationTest {

    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "it"
  }
}
