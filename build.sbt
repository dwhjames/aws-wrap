organization in ThisBuild := "com.github.dwhjames"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion in ThisBuild := "2.11.8"

crossScalaVersions in ThisBuild := Seq("2.10.5", "2.11.8", "2.12.0")

shellPrompt in ThisBuild := CustomShellPrompt.customPrompt

resolvers in ThisBuild ++= Seq(
  "typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "sonatype" at "http://oss.sonatype.org/content/repositories/releases"
)

lazy val awsWrap = project in file(".")

name := "aws-wrap"

libraryDependencies ++= Seq(
  Dependencies.Compile.awsJavaSDK_cloudwatch % "provided",
  Dependencies.Compile.awsJavaSDK_dynamodb % "provided",
  Dependencies.Compile.awsJavaSDK_s3 % "provided",
  Dependencies.Compile.awsJavaSDK_ses % "provided",
  Dependencies.Compile.awsJavaSDK_simpledb % "provided",
  Dependencies.Compile.awsJavaSDK_sns % "provided",
  Dependencies.Compile.awsJavaSDK_sqs % "provided",
  Dependencies.Compile.slf4j,
  Dependencies.Test.scalaTest
)

bintrayPackageLabels := Seq("aws", "dynamodb", "s3", "ses", "simpledb", "sns", "sqs", "async", "future")

lazy val awsWrapTest = project.
  in(file("integration")).
  configs(IntegrationTest).
  dependsOn(awsWrap)
