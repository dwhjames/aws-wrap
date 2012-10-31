package aws.sqs

import scala.util.{Try, Success, Failure}
import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object DynamoDBSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import java.util.concurrent.TimeUnit._

  import aws.core._
  import aws.core.Types._

  implicit val region = SQSRegion.EU_WEST_1

  def ensureSuccess[T](r: Result[SQSMeta, T]) = r match {
    case Result(_, _) => success
    case AWSError(code, message) => failure(message)
  }

  "SQS API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "List queues" in {
      val r = Await.result(SQS.listQueues(), Duration(30, SECONDS))
      ensureSuccess(r)
    }

  }
}