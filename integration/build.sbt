
name := "aws-wrap-test"


libraryDependencies ++= Seq(
  Dependencies.Compile.awsJavaSDK_dynamodb % "it",
  Dependencies.Compile.awsJavaSDK_s3 % "it",
  Dependencies.Compile.jodaTime % "it",
  Dependencies.Compile.jodaConvert % "it",
  Dependencies.Compile.logback % "it",
  Dependencies.IntegrationTest.scalaTest
)


Defaults.itSettings

parallelExecution in IntegrationTest := false


testOptions in IntegrationTest += Tests.Setup { () =>
    println("Start DynamoDB Local")
    System.setProperty("DynamoDB.localMode", "true")
    Process("bash start-dynamodb-local.sh").!
    println("Start fakes3")
    Process("bash start-fakes3.sh").!
}

testOptions in IntegrationTest += Tests.Cleanup { () =>
    println("Stop DynamoDB Local")
    System.clearProperty("DynamoDB.localMode")
    Process("bash stop-dynamodb-local.sh").!
    println("Stop fakes3")
    Process("bash stop-fakes3.sh").!
}
