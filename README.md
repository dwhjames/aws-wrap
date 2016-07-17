# Asynchronous Scala Clients for Amazon Web Services

[![Build Status](https://travis-ci.org/mingchuno/aws-wrap.svg?branch=master)](https://travis-ci.org/mingchuno/aws-wrap)

Asynchronous clients are provided for the following services:

 * DynamoDB
 * CloudWatch
 * Simple Storage Service (S3)
 * Simple Email Service (SES)
 * SimpleDB
 * Simple Notification Service (SNS)
 * Simple Queue Service (SQS)

## Install

aws-wrap is built for both Scala 2.10.x and 2.11.x against AWS Java SDK 1.11.x (for AWS Java SDK 1.10.x, 1.9.x and 1.8.x series, use the 0.8.0, 0.7.3 and 0.6.4 releases of aws-wrap, respectively). Binary releases are available from [Bintray]('https://bintray.com/mingchuno/maven/aws-wrap/view?source=watch').

If you are using SBT, simply add the following to your `build.sbt` file:

```
resolvers += Resolver.bintrayRepo("mingchuno", "maven")

libraryDependencies += "com.github.dwhjames" %% "aws-wrap" % "0.9.0"
```

## Usage

Basically this libary is a thin wrap around offical AWS Java SDK. Take SNS as an example

```
val javaClient = new AmazonSNSAsyncClient() // AWS Java Client
val scalaClient = new AmazonSNSScalaClient(javaClient)
val request = new CreateTopicRequest("topic_name") // good old AWS request type
val result: Future[CreateTopicResult] = scalaClient.createTopic(request) // It return a future
val result2: Future[CreateTopicResult] = scalaClient.createTopic("topic_name") // This is a shortcut

```

## Develop

`sbt compile test` for the core project. If you want to run the integration test `sbt awsWrapTest/it:compile && sbt awsWrapTest/it:test`

## License

Copyright © 2012-2015 Pellucid Analytics.
Copyright © 2015 Daniel W. H. James.
Copyright © 2016 M.C. Or.

This software is distributed under the [Apache License, Version 2.0](LICENSE).
