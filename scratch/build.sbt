
libraryDependencies ++= Seq(
  Dependencies.Compile.awsJavaSDK_dynamodb,
  Dependencies.Compile.awsJavaSDK_s3,
  Dependencies.Compile.jodaTime,
  Dependencies.Compile.jodaConvert,
  Dependencies.Compile.logback
)

publish := ()

publishLocal := ()
