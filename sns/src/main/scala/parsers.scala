package aws.sns

import play.api.libs.json._
import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object SNSParsers {
  import scala.xml.Node
  import language.postfixOps

  implicit def snsMetaParser = Parser[SNSMeta] { r =>
    Success(SNSMeta(r.xml \\ "RequestId" text))
  }

  implicit def listTopicsResultParser = Parser[ListTopicsResult] { r: Response =>
    Success(ListTopicsResult(
      (r.xml \\ "TopicArn").map(_.text),
      (r.xml \\ "NextToken").headOption.map(_.text)))
  }

  implicit def createTopicsResultParser = Parser[CreateTopicResult] { r: Response =>
    Success(
      CreateTopicResult((r.xml \\ "TopicArn").text))
  }

  implicit def subscribeResultParser = Parser[SubscriptionResult] { r: Response =>
    Success(
      SubscriptionResult((r.xml \\ "SubscriptionArn").text))
  }

  implicit def publishResultParser = Parser[PublishResult] { r: Response =>
    Success(
      PublishResult((r.xml \\ "MessageId").text))
  }

  implicit def topicAttributesResultParser = Parser[TopicAttributesResult] { r: Response =>
    val entries = parseAttributes(r.xml \\ "Attributes" head)
    Success(TopicAttributesResult(
      entries("TopicArn"),
      entries("Owner"),
      entries("DisplayName"),
      entries("SubscriptionsPending").toInt,
      entries("SubscriptionsConfirmed").toInt,
      entries("SubscriptionsDeleted").toInt,
      entries.get("Policy").map(Json.parse(_)),
      entries.get("DeliveryPolicy").map(Json.parse(_)),
      entries.get("EffectiveDeliveryPolicy").map(Json.parse(_))))
  }

  implicit def subscriptionAttributesResultParser = Parser[SubscriptionAttributesResult] { r: Response =>
    val entries = parseAttributes(r.xml \\ "Attributes" head)
    Success(SubscriptionAttributesResult(
      entries("SubscriptionArn"),
      entries("TopicArn"),
      entries("Owner"),
      entries.get("ConfirmationWasAuthenticated").map(_.toLowerCase == "true").getOrElse(false),
      entries.get("DeliveryPolicy").map(Json.parse(_)),
      entries.get("EffectiveDeliveryPolicy").map(Json.parse(_))))
  }

  implicit def subscriptionListResultParser = Parser[SubscriptionListResult] { r: Response =>
    Success(
      SubscriptionListResult(
        (r.xml \\ "Subscriptions").map(parseSubscription(_)).flatten,
        (r.xml \\ "NextToken").headOption.map(_.text)))
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SNSMeta, T]] =
    Parser.xmlErrorParser[SNSMeta].or(Parser.resultParser(snsMetaParser, p))

  // Transform a set of <entry><key>xxx</key><value>yyy</value></entry> into a Map
  private def parseAttributes(node: Node): Map[String, String] = {
    (node \ "entry").map { n =>
      ((n \ "key").text) -> (n \ "value").text
    }.toMap
  }

  private def parseSubscription(node: Node): Option[Subscription] = for (
    topicArn <- (node \\ "TopicArn").headOption.map(_.text);
    subscriptionArn <- (node \\ "SubscriptionArn").headOption.map(_.text);
    owner <- (node \\ "Owner").headOption.map(_.text);
    endpoint <- (node \\ "Endpoint").headOption.map(_.text);
    protocol <- (node \\ "Protocol").headOption.map(_.text)
  ) yield Subscription(topicArn, subscriptionArn, owner, Endpoint(endpoint, protocol))

}
