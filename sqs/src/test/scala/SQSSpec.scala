package aws.sqs

import scala.util.{Try, Success, Failure}
import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._


import org.specs2.mutable._

object SQSSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import java.util.concurrent.TimeUnit._


  implicit val region = SQSRegion.EU_WEST_1

  def ensureSuccess[T](r: Result[SQSMeta, T]) = r match {
    case Result(_, _) => success
    case AWSError(_, _, message) => failure(message)
  }

  object Cake extends AWS with SQSLayer
  import Cake._

  "SQS API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "List queues" in {
      val r = Await.result(SQS.listQueues(), Duration(30, SECONDS))
      ensureSuccess(r)
    }

    "Create and delete queue" in {
      Await.result(SQS.createQueue("test-create-queue"), Duration(30, SECONDS)) match {
        case Result(_, queue) =>
          Await.result(SQS.getQueue("test-create-queue"), Duration(30, SECONDS)) match {
            case AWSError(_, _, message) => failure(message)
            case Result(_, q2) => q2.url must be equalTo(queue.url)
          }
          val r2 = Await.result(SQS.deleteQueue(queue.url), Duration(30, SECONDS))
          ensureSuccess(r2)
        case AWSError(_, _, message) => failure(message)
        case _ => failure
      }
    }

    "Add and remove permissions" in {
      Await.result(SQS.createQueue("test-permissions"), Duration(30, SECONDS)) match {
        case Result(_, queue) =>
          val r = Await.result(SQS.addPermission(queue,"new-permission",
                                                     Seq("056023575103"),
                                                     Seq(ActionName.GetQueueUrl)
                                                     ), Duration(30, SECONDS))
          ensureSuccess(r)
          val r2 = Await.result(SQS.removePermission(queue,"new-permission"), Duration(30, SECONDS))
          ensureSuccess(r2)
          val delRes = Await.result(SQS.deleteQueue(queue.url), Duration(30, SECONDS))
          ensureSuccess(delRes)
        case AWSError(_, _, message) => failure(message)
        case _ => failure
      }
    }

    "Set and get attributes" in {
      Await.result(SQS.createQueue("test-attributes", MaximumMessageSize(20 * 1024)), Duration(30, SECONDS)) match {
        case Result(_, queue) =>
          val r = Await.result(SQS.setAttributes(queue, Seq(MaximumMessageSize(10 * 1024))), Duration(30, SECONDS))
          ensureSuccess(r)
          val delRes = Await.result(SQS.deleteQueue(queue.url), Duration(30, SECONDS))
          ensureSuccess(delRes)
        case AWSError(_, _, message) => failure(message)
        case _ => failure
      }
    }

    "Send a message and get the message" in {

      Await.result(SQS.createQueue("test-send-message"), Duration(30, SECONDS)) match {
        case Result(_, queue) =>
          val r = Await.result(SQS.addPermission(queue,"new-permission",
                                                     Seq("056023575103"),
                                                     Seq(ActionName.GetQueueUrl)
                                                     ), Duration(30, SECONDS))
          ensureSuccess(r)
          val r2 = Await.result(SQS.sendMessage(queue,"Hello, World!"), Duration(30, SECONDS))
          ensureSuccess(r2)

          val r3 = Await.result(SQS.receiveMessage(queue), Duration(30, SECONDS))
          ensureSuccess(r3)
          val Result(_, List(MessageReceive(_, str, _, _, _))) = r3
          str must be equalTo("Hello, World!")
          val delRes = Await.result(SQS.deleteQueue(queue.url), Duration(30, SECONDS))
          ensureSuccess(delRes)
        case AWSError(_, _, message) => failure(message)
        case _ => failure
      }
    }

  }
}