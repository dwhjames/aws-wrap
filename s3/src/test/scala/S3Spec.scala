package aws.s3

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object S3Spec extends Specification {

  import scala.concurrent._
  import scala.concurrent.util._
  import java.util.concurrent.TimeUnit._

  implicit val region = S3Region.EU_WEST_1

  "S3 API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult[M <: Metadata, T](r: Result[M, T]) = r match {
      case Errors(errors) => failure(errors.toString)
      case Result(_, _) => success
    }

    "Create a bucket" in {
      val res = Await.result(S3.createBucket(AWS.key + "testbucket"), Duration(30, SECONDS))
      checkResult(res)
    }
  }
}