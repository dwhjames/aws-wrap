package aws.sns

import java.util.Date

import scala.annotation.implicitNotFound
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

object SNSRegion {

  val NAME = "sns"

  val US_EAST_1 = AWSRegion.US_EAST_1(NAME)
  val US_WEST_1 = AWSRegion.US_WEST_1(NAME)
  val US_WEST_2 = AWSRegion.US_WEST_2(NAME)
  val EU_WEST_1 = AWSRegion.EU_WEST_1(NAME)
  val ASIA_SOUTHEAST_1 = AWSRegion.ASIA_SOUTHEAST_1(NAME)
  val ASIA_NORTHEAST_1 = AWSRegion.ASIA_NORTHEAST_1(NAME)
  val SA_EAST_1 = AWSRegion.SA_EAST_1(NAME)

  implicit val DEFAULT = US_EAST_1
}

object SNS extends V2[SNSMeta] {

  override val VERSION = "2010-03-31"

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

  def addPermission(topicArn: String, label: String, awsAccounts: Seq[String], actions: Seq[Action])(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    val params = Seq(
      Action("AddPermission"),
      TopicArn(topicArn),
      Label(label)) ++ AWSAccounts(awsAccounts) ++ ActionList(actions)
    get[Unit](params: _*)
  }

  def confirmSubscription(topicArn: String, token: String, authenticateOnUnsubscribe: Boolean = false)(implicit region: AWSRegion): Future[Result[SNSMeta, SubscriptionResult]] = {
    get[SubscriptionResult](
      Action("ConfirmSubscription"),
      TopicArn(topicArn),
      AuthenticateOnUnsubscribe(authenticateOnUnsubscribe))
  }

  def createTopic(name: String)(implicit region: AWSRegion): Future[Result[SNSMeta, CreateTopicResult]] = {
    get[CreateTopicResult](Action("CreateTopic"), Name(name))
  }

  def deleteTopic(topicArn: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("DeleteTopic"),
      TopicArn(topicArn))
  }

  // GetSubscriptionAttributes

  // GetTopicAttributes

  def listSubscriptions(nextToken: Option[String] = None)(implicit region: AWSRegion): Future[Result[SNSMeta, SubscriptionListResult]] = {
    val params = Seq(Action("ListSubscriptions")) ++ NextToken(nextToken)
    get[SubscriptionListResult](params: _*)
  }

  def listSubscriptionsByTopic(topicArn: String, nextToken: Option[String] = None)(implicit region: AWSRegion): Future[Result[SNSMeta, SubscriptionListResult]] = {
    val params = Seq(
      Action("ListSubscriptionsByTopic"),
      TopicArn(topicArn)) ++ NextToken(nextToken)
    get[SubscriptionListResult](params: _*)
  }

  def listTopics(nextToken: Option[String] = None)(implicit region: AWSRegion): Future[Result[SNSMeta, ListTopicsResult]] = {
    val params = Seq(Action("ListTopics")) ++ NextToken(nextToken)
    get[ListTopicsResult](params: _*)
  }

  def publish(topicArn: String,
              message: Message,
              subject: Option[String] = None)(implicit region: AWSRegion): Future[Result[SNSMeta, PublishResult]] = {
    val params = Seq(Action("Publish"), TopicArn(topicArn)) ++ MessageParameters(message) ++ Subject(subject)
    get[PublishResult](params: _*)
  }

  def removePermission(topicArn: String, label: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("RemovePermission"),
      TopicArn(topicArn),
      Label(label)
    )
  }

  // SetSubscriptionAttributes

  private def setTopicAttributes(topicArn: String,
                                attributeName: String,
                                attributeValue: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("SetTopicAttribute"),
      TopicArn(topicArn),
      AttributeName(attributeName),
      AttributeValue(attributeValue)
    )
  }

  def setTopicDisplayName(topicArn: String, name: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] =
    setTopicAttributes(topicArn, "DisplayName", name)

  def setTopicPolicy(topicArn: String, policy: JsValue)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] =
    setTopicAttributes(topicArn, "Policy", policy.toString)

  def setTopicDeliveryPolicy(topicArn: String, deliveryPolicy: JsValue)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] =
    setTopicAttributes(topicArn, "DisplayName", deliveryPolicy.toString)

  def subscribe(endpoint: Endpoint, topicArn: String)(implicit region: AWSRegion): Future[Result[SNSMeta, SubscriptionResult]] = {
    val params = Seq(Action("Subscribe"), TopicArn(topicArn)) ++ EndpointProtocol(endpoint)
    get[SubscriptionResult](params: _*)
  }

  def unsubscribe(subscriptionArn: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](Action("Unsubscribe"), SubscriptionArn(subscriptionArn))
  }

}

