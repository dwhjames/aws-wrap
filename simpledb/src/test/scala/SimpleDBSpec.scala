package aws.simpledb

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

  implicit val region = SimpleDB.Regions.EU_WEST_1

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult(r: Try[Result]) = r match {
      case Success(result) =>
        result.metadata.requestId must not be empty
      case Failure(Error(SimpleResult(_, errors))) => failure(errors.toString)
      case Failure(t) => failure(t.getMessage)
    }

    "Create a domain" in {
      val r = Await.result(SimpleDB.createDomain("test-domaine-create"), Duration(30, SECONDS))
      checkResult(r)
    }

    "Delete a domain" in {
      val r = Await.result(SimpleDB.createDomain("test-domaine-delete"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.deleteDomain("test-domaine-delete"), Duration(30, SECONDS)))
      checkResult(r)
    }

    "List domains" in {
      val r = Await.result(SimpleDB.listDomains(), Duration(30, SECONDS))
      checkResult(r)
      for(domains <- r)
        domains.body must not be empty
      success
    }
  }
}