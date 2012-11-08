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

import java.util.Date

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import play.api.libs.ws._
import play.api.libs.json._

import aws.core._
import aws.core.Types._
import aws.core.parsers._
import aws.core.signature.V2

import aws.sns.SNSParsers._

case class SNSMeta(requestId: String) extends Metadata

object SNS extends V2[SNSMeta](version = "2010-03-31") {

  object Parameters {
    def NextToken(nextToken: Option[String]): Seq[(String, String)] = nextToken.toSeq.map("NextToken" -> _)
    def Name(name: String) = ("Name" -> name)
    def TopicArn(arn: String) = ("TopicArn" -> arn)
    def Label(name: String) = ("Label" -> name)
    def AttributeName(name: String) = ("AttributeName" -> name)
    def AttributeValue(value: String) = ("AttributeValue" -> value)
    def EndpointProtocol(endpoint: Endpoint) = Seq(
      "Endpoint" -> endpoint.value,
      "Protocol" -> endpoint.protocol)
    def SubscriptionArn(arn: String) = ("SubscriptionArn" -> arn)
    def AuthenticateOnUnsubscribe(auth: Boolean) = ("AuthenticateOnUnsubscribe" -> (if (auth) "true" else "false"))
    def AWSAccounts(accounts: Seq[String]): Seq[(String, String)] = (for ((account, i) <- accounts.zipWithIndex) yield {
      (("AWSAccountId.member." + (i + 1)) -> account)
    })
    def ActionList(actions: Seq[Action]): Seq[(String, String)] = (for ((action, i) <- actions.zipWithIndex) yield {
      (("ActionName.member." + (i + 1)) -> action.toString)
    })
    def MessageParameters(message: Message) =
      Seq("Message" -> message.serialize) ++ (if (message.json) Seq("MessageStructure" -> "json") else Nil)
    def Subject(subject: Option[String]) = subject.toSeq.map("Subject" -> _)
  }

  import AWS.Parameters._
  import Parameters._

  /**
   * Adds a statement to a topic's access control policy,
   * granting access for the specified AWS accounts to the specified actions.
   *
   * @param topicArn The ARN of the topic whose access control policy you wish to modify.
   * @param label A unique identifier for the new policy statement.
   * @param awsAccounts The AWS account IDs of the users (principals) who will be given access to the specified actions.
   *                    The users must have AWS accounts, but do not need to be signed up for this service.
   * @param actions The action you want to allow for the specified principal(s).
   */
  def addPermission(topicArn: String, label: String, awsAccounts: Seq[String], actions: Seq[Action])(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] = {
    val params = Seq(
      Action("AddPermission"),
      TopicArn(topicArn),
      Label(label)) ++ AWSAccounts(awsAccounts) ++ ActionList(actions)
    get[Unit](params: _*)
  }

  /**
   * Verifies an endpoint owner's intent to receive messages by validating the token
   * sent to the endpoint by an earlier Subscribe action. If the token is valid, the action creates a new subscription and
   * returns its Amazon Resource Name (ARN).
   *
   * @param topicArn The ARN of the topic for which you wish to confirm a subscription.
   * @param token Short-lived token sent to an endpoint during the Subscribe action.
   * @param authenticateOnUnsubscribe Disallows unauthenticated unsubscribes of the subscription.
   *        If the value of this parameter is true and the request has an AWS signature,
   *        then only the topic owner and the subscription owner can unsubscribe the endpoint.
   */
  def confirmSubscription(topicArn: String, token: String, authenticateOnUnsubscribe: Boolean = false)(implicit region: SNSRegion): Future[Result[SNSMeta, SubscriptionResult]] = {
    get[SubscriptionResult](
      Action("ConfirmSubscription"),
      TopicArn(topicArn),
      AuthenticateOnUnsubscribe(authenticateOnUnsubscribe))
  }

  /**
   * Creates a topic to which notifications can be published. Users can create at most 100 topics.
   * For more information, see http://aws.amazon.com/sns. This action is idempotent, so if the requester already owns a topic
   * with the specified name, that topic's ARN will be returned without creating a new topic.
   *
   * Constraints: Topic names must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens,
   * and must be between 1 and 256 characters long.
   *
   * @param name The name of the topic you want to create.
   */
  def createTopic(name: String)(implicit region: SNSRegion): Future[Result[SNSMeta, CreateTopicResult]] = {
    get[CreateTopicResult](Action("CreateTopic"), Name(name))
  }

  /**
   * Deletes a topic and all its subscriptions.
   * Deleting a topic might prevent some messages previously sent to the topic from being delivered to subscribers.
   * This action is idempotent, so deleting a topic that does not exist will not result in an error.
   *
   * @param topicArn The ARN of the topic you want to delete.
   */
  def deleteTopic(topicArn: String)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("DeleteTopic"),
      TopicArn(topicArn))
  }

  /**
   * Returns all of the properties of a subscription.
   *
   * @param subscriptionArn The ARN of the subscription whose properties you want to get.
   */
  def getSubscriptionAttributes(subscriptionArn: String)(implicit region: SNSRegion): Future[Result[SNSMeta, SubscriptionAttributesResult]] = {
    get[SubscriptionAttributesResult](
      Action("SubscriptionAttributesResult"),
      SubscriptionArn(subscriptionArn))
  }

  /**
   * Returns all of the properties of a topic.
   * Topic properties returned might differ based on the authorization of the user.
   *
   * @param topicArn The ARN of the topic whose properties you want to get.
   */
  def getTopicAttributes(topicArn: String)(implicit region: SNSRegion): Future[Result[SNSMeta, TopicAttributesResult]] = {
    get[TopicAttributesResult](Action("GetTopicAttributes"), TopicArn(topicArn))
  }

  /**
   * Returns a list of the requester's subscriptions.
   * Each call returns a limited list of subscriptions, up to 100.
   * If there are more subscriptions, a NextToken is also returned.
   * Use the NextToken parameter in a new ListSubscriptions call to get further results.
   *
   * @param nextToken Token returned by the previous `listSubscriptions` request.
   */
  def listSubscriptions(nextToken: Option[String] = None)(implicit region: SNSRegion): Future[Result[SNSMeta, SubscriptionListResult]] = {
    val params = Seq(Action("ListSubscriptions")) ++ NextToken(nextToken)
    get[SubscriptionListResult](params: _*)
  }

  /**
   * Returns a list of the subscriptions to a specific topic.
   * Each call returns a limited list of subscriptions, up to 100.
   * If there are more subscriptions, a `nextToken` is also returned.
   * Use the `nextToken` parameter in a new `listSubscriptionsByTopic` call to get further results.
   *
   * @param topicArn The ARN of the topic for which you wish to find subscriptions.
   * @param nextToken Token returned by the previous 'listSubscriptionsByTopic` request.
   */
  def listSubscriptionsByTopic(topicArn: String, nextToken: Option[String] = None)(implicit region: SNSRegion): Future[Result[SNSMeta, SubscriptionListResult]] = {
    val params = Seq(
      Action("ListSubscriptionsByTopic"),
      TopicArn(topicArn)) ++ NextToken(nextToken)
    get[SubscriptionListResult](params: _*)
  }

  /**
   * Returns a list of the requester's topics. Each call returns a limited list of topics, up to 100.
   * If there are more topics, a `nextToken` is also returned.
   * Use the `nextToken` parameter in a new `listTopics` call to get further results.
   *
   * @param nextToken Token returned by the previous `listTopics` request.
   */
  def listTopics(nextToken: Option[String] = None)(implicit region: SNSRegion): Future[Result[SNSMeta, ListTopicsResult]] = {
    val params = Seq(Action("ListTopics")) ++ NextToken(nextToken)
    get[ListTopicsResult](params: _*)
  }

  /**
   * Sends a message to all of a topic's subscribed endpoints.
   * When a `messageId` is returned, the message has been saved and Amazon SNS will attempt to deliver it to the topic's subscribers shortly.
   * The format of the outgoing message to each subscribed endpoint depends on the notification protocol selected.
   *
   * @param topicArn The topic you want to publish to.
   * @param message The message you want to send to the topic.
   * @param subject Optional parameter to be used as the "Subject" line of when the message is delivered to e-mail endpoints.
   *                This field will also be included, if present, in the standard JSON messages delivered to other endpoints.
   *                Constraints: Subjects must be ASCII text that begins with a letter, number or punctuation mark;
   *                must not include line breaks or control characters; and must be less than 100 characters long.
   */
  def publish(topicArn: String,
              message: Message,
              subject: Option[String] = None)(implicit region: SNSRegion): Future[Result[SNSMeta, PublishResult]] = {
    val params = Seq(Action("Publish"), TopicArn(topicArn)) ++ MessageParameters(message) ++ Subject(subject)
    get[PublishResult](params: _*)
  }

  /**
   * The RemovePermission action removes a statement from a topic's access control policy.
   *
   * @param topicArn The ARN of the topic whose access control policy you wish to modify.
   * @param label The unique label of the statement you want to remove.

   */
  def removePermission(topicArn: String, label: String)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("RemovePermission"),
      TopicArn(topicArn),
      Label(label))
  }

  private def setSubscriptionAttributes(subscriptionArn: String,
                                        attributeName: String,
                                        attributeValue: JsValue)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("SetSubscriptionAttributes"),
      SubscriptionArn(subscriptionArn),
      AttributeName(attributeName),
      AttributeValue(attributeValue.toString))
  }

  def setSubscriptionDeliveryPolicy(subscriptionArn: String,
                                    deliveryPolicy: JsValue)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] =
    setSubscriptionAttributes(subscriptionArn, "DeliveryPolicy", deliveryPolicy)

  private def setTopicAttributes(topicArn: String,
                                 attributeName: String,
                                 attributeValue: String)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("SetTopicAttributes"),
      TopicArn(topicArn),
      AttributeName(attributeName),
      AttributeValue(attributeValue))
  }

  def setTopicDisplayName(topicArn: String, name: String)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] =
    setTopicAttributes(topicArn, "DisplayName", name)

  def setTopicPolicy(topicArn: String, policy: JsValue)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] =
    setTopicAttributes(topicArn, "Policy", policy.toString)

  def setTopicDeliveryPolicy(topicArn: String, deliveryPolicy: JsValue)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] =
    setTopicAttributes(topicArn, "DisplayName", deliveryPolicy.toString)

  /**
   * The Subscribe action prepares to subscribe an endpoint by sending the endpoint a confirmation message.
   * To actually create a subscription, the endpoint owner must call the ConfirmSubscription action with the token
   * from the confirmation message. Confirmation tokens are valid for three days.
   *
   * @param endpoint The endpoint that you want to receive notifications.
   * @param topicArn The ARN of topic you want to subscribe to.
   */
  def subscribe(endpoint: Endpoint, topicArn: String)(implicit region: SNSRegion): Future[Result[SNSMeta, SubscriptionResult]] = {
    val params = Seq(Action("Subscribe"), TopicArn(topicArn)) ++ EndpointProtocol(endpoint)
    get[SubscriptionResult](params: _*)
  }

  def unsubscribe(subscriptionArn: String)(implicit region: SNSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](Action("Unsubscribe"), SubscriptionArn(subscriptionArn))
  }

}

