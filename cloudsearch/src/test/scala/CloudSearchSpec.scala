package aws.cloudsearch

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object TestUtils extends Specification { // Evil hack to access Failure

  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import java.util.concurrent.TimeUnit._

  implicit val region = CloudSearchRegion.US_EAST_1

  def checkResult[M <: Metadata, T](r: Result[M, T]) = r match {
    case AWSError(code, message) => failure(message)
    case Result(_, _) => success
  }

  def waitFor[T](f: Future[T]) = Await.result(f, Duration(30, SECONDS))
}

object CloudSearchSpec extends Specification {

  import TestUtils._

  val domain = ("imdb-movies", "5d3sfdtvri2lghw27klaho756y")

  "CloudSearch API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "Search" in {
      val r = waitFor(CloudSearch.search(
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title")))
      println(r.body)
    }
  }
}