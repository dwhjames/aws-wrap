package aws.sns

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object SNSSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.util._
  import java.util.concurrent.TimeUnit._

  implicit val region = SNSRegion.EU_WEST_1

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult[T](r: Result[SNSMeta, T]) = r match {
      case Errors(errors) => failure(errors.toString)
      case Result(SNSMeta(requestId), _) => requestId must not be empty
    }

    "Create a topic" in {
      val r = Await.result(SNS.createTopic("test-topic-create"), Duration(30, SECONDS))
      checkResult(r)
    }

    "Delete a topic" in {
      val r = Await.result(SNS.createTopic("test-topic-delete"), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, result) => 
          Await.result(SNS.deleteTopic(result.topicArn), Duration(30, SECONDS))
      }
      checkResult(r)
    }

    "List topics" in {
      val topicArn = Await.result(SNS.createTopic("test-topic-list"), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, result) => {
          val newTopic = result.topicArn
          Await.result(SNS.listTopics(), Duration(30, SECONDS)) match {
            case Errors(errors) => failure(errors.toString)
            case Result(_, listresult) => listresult.topics.exists(_ == newTopic) must beEqualTo(true)
          }
        }
      }
    }

    "Subscribe" in {
      val subscribeFuture = SNS.createTopic("test-subsciptions").flatMap(_ match {
        case e@Errors(errors) => Future.successful(e)
        case Result(_, createRes) => SNS.subscribe(Endpoint.Http("http://example.com"), createRes.topicArn)
      })

      val r = Await.result(subscribeFuture, Duration(30, SECONDS))
      checkResult(r)
    }

    // Deactivated because a confirmation is necessary to actually create the subscription (so we can't unsubscribe)
/*    "Unsubscribe" in {
      val unsubscribeFuture = SNS.createTopic("test-subsciptions").flatMap(_ match {
        case e@Errors(errors) => Future.successful(e)
        case Result(_, createRes) => SNS.subscribe(Endpoint.Http("http://example.com"), createRes.topicArn)
      }).flatMap(_ match {
        case e@Errors(errors) => Future.successful(e)
        case Result(_, subscribeRes) => SNS.unsubscribe(subscribeRes.subscriptionArn)
      })

      val r = Await.result(unsubscribeFuture, Duration(30, SECONDS))
      checkResult(r)
    }*/

  }
}