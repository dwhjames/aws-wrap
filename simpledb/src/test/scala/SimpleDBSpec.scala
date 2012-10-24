package aws.simpledb

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object SimpleDBSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.util._
  import java.util.concurrent.TimeUnit._

  implicit val region = SDBRegion.EU_WEST_1

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult[T](r: Result[SimpleDBMeta, T]) = r match {
      case AWSError(code, message) => failure(message)
      case Result(SimpleDBMeta(requestId, _), _) => requestId must not be empty
    }

    "Create a domain" in {
      val r = Await.result(SimpleDB.createDomain("test-domain-create"), Duration(30, SECONDS))
      checkResult(r)
    }

    "Delete a domain" in {
      val r = Await.result(SimpleDB.createDomain("test-domain-delete"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.deleteDomain("test-domain-delete"), Duration(30, SECONDS)))
      checkResult(r)
    }

    "List domains" in {
      val r = Await.result(SimpleDB.listDomains(), Duration(30, SECONDS))
      checkResult(r)
      for(body <- r)
        body must not be empty
      success
    }

    "Put attributes" in {
      val toto = SDBAttribute("toto", "tata")
      val r = Await.result(SimpleDB.createDomain("test-put-attributes"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.putAttributes("test-put-attributes", "theItem", Seq(toto)), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.getAttributes("test-put-attributes", "theItem", None, true), Duration(30, SECONDS)))
      r match {
        case AWSError(code, message) => failure(message)
        case Result(_, body) if body.contains(toto) => success
        case Result(_, body) => failure("Could not find inserted attribute in " + body)
      }

      val d = Await.result(SimpleDB.deleteDomain("test-put-attributes"), Duration(30, SECONDS))
      checkResult(d)
    }

    "Delete attributes" in {
      val foobar = SDBItem("foobar", Nil)
      val r = Await.result(SimpleDB.createDomain("test-delete-attributes"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.putAttributes("test-delete-attributes", foobar.name, Seq(SDBAttribute("toto", "tata"))), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.deleteAttributes("test-delete-attributes", foobar.name), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.deleteDomain("test-delete-attributes"), Duration(30, SECONDS)))
      checkResult(r)
    }

    "Select attributes" in {
      val toto = SDBAttribute("toto", "tata")
      val r = Await.result(SimpleDB.createDomain("test-select-attributes"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.putAttributes("test-select-attributes", "theItem", Seq(toto)), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.select("select * from `test-select-attributes`", None, true), Duration(30, SECONDS)))
      r match {
        case AWSError(code, message) => failure(message)
        case Result(_, body) if body.exists(item => item.name == "theItem" && item.attributes.contains(toto)) => success
        case Result(_, body) => failure("Could not find inserted attribute in " + body)
      }

      val d = Await.result(SimpleDB.deleteDomain("test-select-attributes"), Duration(30, SECONDS))
      checkResult(d)
    }

    "Batch operations" in {
      val color = SDBAttribute("color", "green")
      val foobar = SDBItem("foobar", Seq(color))
      val r = Await.result(SimpleDB.createDomain("test-batch"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.batchPutAttributes("test-batch", Seq(foobar)), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.select("select * from `test-batch`", None, true), Duration(30, SECONDS)))
      r match {
        case AWSError(code, message) =>
          failure(message)
        case Result(_, body) if body.exists(item => item.name == "foobar" && item.attributes.contains(color)) =>
          success
        case Result(_, body) =>
          failure("Could not find inserted attribute in " + body)

      }

      val r2 = Await.result(SimpleDB.batchDeleteAttributes("test-batch", Seq(foobar)), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.select("select * from `test-batch`", None, true), Duration(30, SECONDS)))
      r2 match {
        case AWSError(code, message) =>
          failure(message)
        case Result(_, body) if body.exists(item => item.name == "foobar" && item.attributes.contains(color)) =>
          failure("Batch deletion failed: " + body)
        case Result(_, body) =>
          success
      }

      val d = Await.result(SimpleDB.deleteDomain("test-batch"), Duration(30, SECONDS))
      checkResult(d)
    }

  }
}