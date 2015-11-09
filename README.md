[![Build Status](https://travis-ci.org/dwhjames/aws-wrap.svg?branch=master)](https://travis-ci.org/dwhjames/aws-wrap)

## Asynchronous Scala Clients for Amazon Web Services

Asynchronous clients are provided for the following services:

 * DynamoDB
 * CloudWatch
 * Simple Storage Service (S3)
 * Simple Email Service (SES)
 * SimpleDB
 * Simple Notification Service (SNS)
 * Simple Queue Service (SQS)

## Usage

aws-wrap is built for both Scala 2.10.x and 2.11.x against AWS Java SDK 1.10.x (for AWS Java SDK 1.9.x and 1.8.x series, use the 0.7.3 and 0.6.4 releases of aws-wrap, respectively). Binary releases are available from [Bintray]('https://bintray.com/dwhjames/maven/aws-wrap/view?source=watch').

<a href='https://bintray.com/dwhjames/maven/aws-wrap/view?source=watch' alt='Get automatic notifications about new "aws-wrap" versions'><img src='https://www.bintray.com/docs/images/bintray_badge_color.png'></a>

If you are using SBT, simply add the following to your `build.sbt` file:

```
resolvers += Resolver.bintrayRepo("dwhjames", "maven")

libraryDependencies += "com.github.dwhjames" %% "aws-wrap" % "0.8.0"
```

## License

Copyright © 2012-2015 Pellucid Analytics.
Copyright © 2015 Daniel W. H. James.

This software is distributed under the [Apache License, Version 2.0](LICENSE).
