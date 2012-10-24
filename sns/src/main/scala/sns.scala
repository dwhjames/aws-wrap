package aws.sns

import java.util.Date

import scala.annotation.implicitNotFound
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

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
    def NextToken(nextToken: Option[String]):Seq[(String, String)] = nextToken.toSeq.map("NextToken" -> _)
    def Name(name: String) = ("Name" -> name)
    def TopicArn(name: String) = ("TopicArn" -> name)
    def EndpointProtocol(endpoint: Endpoint) = Seq(
      "Endpoint" -> endpoint.value,
      "Protocol" -> endpoint.protocol
    )
    def SubscriptionArn(arn: String) = ("SubscriptionArn" -> arn)
  }

  import AWS.Parameters._
  import Parameters._

  // AddPermission

  // ConfirmSubscription

  def createTopic(name: String)(implicit region: AWSRegion): Future[Result[SNSMeta, CreateTopicResult]] = {
    get[CreateTopicResult](Action("CreateTopic"), Name(name))
  }

  def deleteTopic(topicArn: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](
      Action("DeleteTopic"),
      TopicArn(topicArn)
    )
  }

  // GetSubscriptionAttributes

  // GetTopicAttributes

  // ListSubscriptions

  // ListSubscriptionsByTopic

  def listTopics(nextToken: Option[String] = None)(implicit region: AWSRegion): Future[Result[SNSMeta, ListTopicsResult]] = {
    val params = Seq(Action("ListTopics")) ++ NextToken(nextToken)
    get[ListTopicsResult](params:_*)
  }

  // Publish

  // RemovePermission

  // SetSubscriptionAttributes

  // SetTopicAttributes

  def subscribe(endpoint: Endpoint, topicArn: String)(implicit region: AWSRegion): Future[Result[SNSMeta, SubscribeResult]] = {
    val params = Seq(Action("Subscribe"), TopicArn(topicArn)) ++ EndpointProtocol(endpoint)
    get[SubscribeResult](params:_*)
  }

  def unsubscribe(subscriptionArn: String)(implicit region: AWSRegion): Future[EmptyResult[SNSMeta]] = {
    get[Unit](Action("Unsubscribe"), SubscriptionArn(subscriptionArn))
  }

}

