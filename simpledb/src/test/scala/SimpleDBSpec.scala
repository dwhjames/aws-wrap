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

  implicit val region = SDBRegion.EU_WEST_1

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult(r: Try[Result]) = r match {
      case Success(result) => result.metadata.requestId must not be empty
      case Failure(Error(SimpleResult(_, errors))) => failure(errors.toString)
      case Failure(t) => failure(t.getMessage)
    }

    "Create a domain" in {
      val r = Await.result(SimpleDB.createDomain("test-domain-create"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.domainMetadata("test-domain-create"), Duration(30, SECONDS)))
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
      for(domains <- r)
        domains.body must not be empty
      success
    }

    "Put attributes" in {
      val toto = SDBAttribute("toto", "tata")
      val r = Await.result(SimpleDB.createDomain("test-put-attributes"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.putAttributes("test-put-attributes", "theItem", Seq(toto)), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.getAttributes("test-put-attributes", "theItem", None, true), Duration(30, SECONDS)))
      r match {
        case Success(result) if result.body.contains(toto) => success
        case Success(result) => failure("Could not find inserted attribute in " + result.body)
        case Failure(Error(SimpleResult(_, errors))) => failure(errors.toString)
        case Failure(t) => failure(t.getMessage)
      }

      val d = Await.result(SimpleDB.deleteDomain("test-put-attributes"), Duration(30, SECONDS))
      checkResult(d)
    }

    "Delete attributes" in {
      val r = Await.result(SimpleDB.createDomain("test-delete-attributes"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.putAttributes("test-delete-attributes", "theItem", Seq(SDBAttribute("toto", "tata"))), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.deleteAttributes("test-delete-attributes", "theItem"), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.deleteDomain("test-delete-attributes"), Duration(30, SECONDS)))
      checkResult(r)
    }

    "Select attributes" in {
      val toto = SDBAttribute("toto", "tata")
      val r = Await.result(SimpleDB.createDomain("test-select-attributes"), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.putAttributes("test-select-attributes", "theItem", Seq(toto)), Duration(30, SECONDS)))
        .flatMap(_ => Await.result(SimpleDB.select("select * from `test-select-attributes`", None, true), Duration(30, SECONDS)))
      r match {
        case Success(result) if result.body.exists(item => item.name == "theItem" && item.attributes.contains(toto)) => success
        case Success(result) => failure("Could not find inserted attribute in " + result.body)
        case Failure(Error(SimpleResult(_, errors))) => failure(errors.toString)
        case Failure(t) => failure(t.getMessage)
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
        case Success(result) if result.body.exists(item => item.name == "foobar" && item.attributes.contains(color)) =>
          success
        case Success(result) =>
          failure("Could not find inserted attribute in " + result.body)
        case Failure(Error(SimpleResult(_, errors))) =>
          failure(errors.toString)
        case Failure(t) =>
          failure(t.getMessage)
      }

      val r2 = Await.result(SimpleDB.batchDeleteAttributes("test-batch", Seq(foobar)), Duration(30, SECONDS))
        .flatMap(_ => Await.result(SimpleDB.select("select * from `test-batch`", None, true), Duration(30, SECONDS)))
      r2 match {
        case Success(result) if result.body.exists(item => item.name == "foobar" && item.attributes.contains(color)) =>
          failure("Batch deletion failed: " + result.body)
        case Success(result) =>
          success
        case Failure(Error(SimpleResult(_, errors))) =>
          failure(errors.toString)
        case Failure(t) =>
          failure(t.getMessage)
      }

      val d = Await.result(SimpleDB.deleteDomain("test-batch"), Duration(30, SECONDS))
      checkResult(d)
    }


  }
}