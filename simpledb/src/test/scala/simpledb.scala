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

  implicit val region = SimpleDB.Regions.EU_WEST_1

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "Create a domain" in {
      val r = Await.result(SimpleDB.createDomain("testdomain"), Duration(30, SECONDS))
      r.status.shouldEqual(200)
    }
  }
}
