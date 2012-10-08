package aws.dynamodb

import scala.util.{Try, Success, Failure}
import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object SimpleDBSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.util._
  import java.util.concurrent.TimeUnit._

  implicit val region = DDBRegion.EU_WEST_1

  "DynamoDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult(r: Try[Result]) = r match {
      case Success(result) => result.metadata.requestId must not be empty
      case Failure(Error(SimpleResult(_, errors))) => failure(errors.toString)
      case Failure(t) => failure(t.getMessage)
    }

    "List tables" in {
      val r = Await.result(DynamoDB.listTables(), Duration(30, SECONDS))
      println("list tables result = " + r.body)
      r.status should be equalTo(200)
    }

  }
}