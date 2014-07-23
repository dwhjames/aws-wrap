import LocalMode.localMode

name := "aws-wrap-test"


libraryDependencies ++= Seq(
  Dependencies.Compile.awsJavaSDK,
  Dependencies.Compile.jodaTime,
  Dependencies.Compile.jodaConvert,
  Dependencies.Compile.logback,
  Dependencies.IntegrationTest.scalaTest
)


Defaults.itSettings

parallelExecution in IntegrationTest := false


localMode := true

// testOptions in IntegrationTest += Tests.Argument(s"-D=${localMode.value}")

testOptions in IntegrationTest += Tests.Setup { () =>
  if (localMode.value) {
    println("Start DynamoDB Local")
    System.setProperty("DynamoDB.localMode", "true")
    Process("bash start-dynamodb-local.sh") !
  }
}

testOptions in IntegrationTest += Tests.Cleanup { () =>
  if (localMode.value) {
    println("Stop DynamoDB Local")
    System.clearProperty("DynamoDB.localMode")
    Process("bash stop-dynamodb-local.sh") !
  }
}
