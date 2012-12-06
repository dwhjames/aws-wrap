package aws.sns

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object SNSSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import java.util.concurrent.TimeUnit._

  implicit val region = SNSRegion.EU_WEST_1

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult[T](r: Result[SNSMeta, T]) = r match {
      case AWSError(_, code, message) => failure(code + ": " + message)
      case Result(SNSMeta(requestId), _) => requestId must not be empty
    }

    "Parse errors correctly" in {
      Await.result(SNS.addPermission("invalid-arn", "permission", Seq("test@toto.com"), Seq(Action.ListTopics)), Duration(30, SECONDS)) match {
        case Result(_, _) => failure("This is supposed to be an error, the arn is wrong!")
        case AWSError(_, code, message) => code must beEqualTo("InvalidParameter")
      }
    }

    "Create a topic" in {
      val r = Await.result(SNS.createTopic("test-topic-create"), Duration(30, SECONDS))
      checkResult(r)
    }

    "Delete a topic" in {
      val r = Await.result(SNS.createTopic("test-topic-delete"), Duration(30, SECONDS)) match {
        case AWSError(_, code, message) => failure(code + ": " + message)
        case Result(_, result) => Await.result(SNS.deleteTopic(result), Duration(30, SECONDS))
      }
      checkResult(r)
    }

    "List topics" in {
      Await.result(SNS.createTopic("test-topic-list"), Duration(30, SECONDS)) match {
        case AWSError(_, code, message) => failure(code + ": " + message)
        case Result(_, newTopic) => {
          Await.result(SNS.listTopics(), Duration(30, SECONDS)) match {
            case AWSError(_, code, message) => failure(message)
            case Result(_, listresult) => listresult.topics.exists(_ == newTopic) must beEqualTo(true)
          }
        }
      }
    }

    "Subscribe" in {
      val subscribeFuture = SNS.createTopic("test-subsciptions").flatMap(_ match {
        case e@AWSError(_, _, _) => Future.successful(e)
        case Result(_, topicArn) => SNS.subscribe(Endpoint.Http("http://example.com"), topicArn)
      })

      val r = Await.result(subscribeFuture, Duration(30, SECONDS))
      checkResult(r)
    }

    "Publish" in {
      val publishFuture = SNS.createTopic("test-publish").flatMap(_ match {
        case e@AWSError(_, _, _) => Future.successful(e)
        case Result(_, topicArn) => SNS.publish(topicArn, Message("hello, there", Some("just for http")))
      })

      val r = Await.result(publishFuture, Duration(30, SECONDS))
      checkResult(r)
    }

    "Add and remove permissions" in {
      val accounts = Seq("foobar@example.com")
      val actions = Seq(Action.ListTopics)
      val topicArn = Await.result(SNS.createTopic("test-permissions"), Duration(30, SECONDS)) match {
        case AWSError(_, code, message) => failure(code + ": " + message)
        case Result(_, newTopic) => {
          checkResult(Await.result(SNS.addPermission(newTopic, "Foobar", accounts, actions), Duration(30, SECONDS)))
          checkResult(Await.result(SNS.removePermission(newTopic, "Foobar"), Duration(30, SECONDS)))
        }
      }
    }

    "Set/get topic attributes" in {
      val displayName = "Some Display Name"
      Await.result(SNS.createTopic("test-topic-attributes"), Duration(30, SECONDS)) match {
        case AWSError(_, code, message) => failure(code + ": " + message)
        case Result(_, topicArn) => {
          checkResult(Await.result(SNS.setTopicDisplayName(topicArn, displayName), Duration(30, SECONDS)))
          Await.result(SNS.getTopicAttributes(topicArn), Duration(30, SECONDS)) match {
            case AWSError(_, code, message) => failure(code + ": " + message)
            case Result(_, result) => result.displayName must beEqualTo(displayName)
          }
        }
      }
    }

    // Deactivated because a confirmation is necessary to actually create the subscription (so we can't unsubscribe)
/*    "Unsubscribe" in {
      val unsubscribeFuture = SNS.createTopic("test-subsciptions").flatMap(_ match {
        case e@AWSError(_, _) => Future.successful(e)
        case Result(_, createRes) => SNS.subscribe(Endpoint.Http("http://example.com"), createRes.topicArn)
      }).flatMap(_ match {
        case e@AWSError(_, _) => Future.successful(e)
        case Result(_, subscribeRes) => SNS.unsubscribe(subscribeRes.subscriptionArn)
      })

      val r = Await.result(unsubscribeFuture, Duration(30, SECONDS))
      checkResult(r)
    }*/

  }
}