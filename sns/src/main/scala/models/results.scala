package aws.sns

import play.api.libs.json.JsValue

import java.util.Date

case class ListTopicsResult(topics: Seq[String], nextToken: Option[String])

case class CreateTopicResult(topicArn: String)

case class SubscriptionResult(subscriptionArn: String)

case class Subscription(topicArn: String, subscriptionArn: String, owner: String, endpoint: Endpoint)

case class SubscriptionListResult(subscriptions: Seq[Subscription], nextToken: Option[String])

case class PublishResult(messageId: String)

case class TopicAttributesResult(
  topicArn: String, // the topic's ARN
  owner: String, // the AWS account ID of the topic's owner
  displayName: String, // the human-readable name used in the "From" field for notifications to email and email-json endpoints
  subscriptionsPending: Int, // the number of subscriptions pending confirmation on this topic
  subscriptionsConfirmed: Int, // the number of confirmed subscriptions on this topic
  subscriptionsDeleted: Int, // the number of deleted subscriptions on this topic
  policy: Option[JsValue], // the JSON serialization of the topic's access control policy
  deliveryPolicy: Option[JsValue], // the JSON serialization of the topic's delivery policy
  effectiveDeliveryPolicy: Option[JsValue] // the JSON serialization of the effective delivery policy which takes into account system defaults    
  )

