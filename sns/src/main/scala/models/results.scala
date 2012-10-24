package aws.sns

import java.util.Date

case class ListTopicsResult(topics: Seq[String], nextToken: Option[String])

case class CreateTopicResult(topicArn: String)

case class SubscribeResult(subscriptionArn: String)

