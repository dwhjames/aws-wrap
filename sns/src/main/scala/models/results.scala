package aws.sns

import java.util.Date

case class ListTopicsResult(topics: Seq[String], nextToken: Option[String])

case class CreateTopicResult(topicArn: String)

case class SubscriptionResult(subscriptionArn: String)

case class Subscription(topicArn: String, subscriptionArn: String, owner: String, endpoint: Endpoint)

case class SubscriptionListResult(subscriptions: Seq[Subscription], nextToken: Option[String])

