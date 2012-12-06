/*
 * Copyright 2012 Pellucid and Zenexity
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aws.sns

import play.api.libs.json.JsValue

import java.util.Date

case class ListTopicsResult(topics: Seq[String], nextToken: Option[String])

case class Subscription(topicArn: String, subscriptionArn: String, owner: String, endpoint: Endpoint)

case class SubscriptionListResult(subscriptions: Seq[Subscription], nextToken: Option[String])

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

case class SubscriptionAttributesResult(
  subscriptionArn: String, // the subscription's ARN
  topicArn: String, // the topic ARN which the subscription is associated with
  owner: String, // the AWS account ID of the subscription's owner
  confirmationWasAuthenticated: Boolean, // True if the subscription confirmation request was authenticated
  deliveryPolicy: Option[JsValue], // the JSON serialization of the subscription's delivery policy
  effectiveDeliveryPolicy: Option[JsValue] // the JSON serialization of the effective delivery policy which takes into the topic delivery policy and account system defaults
  )

