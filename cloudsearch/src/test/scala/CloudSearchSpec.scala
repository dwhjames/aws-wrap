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
    case AWSError(meta, code, message) => failure(message)
    case Result(_, _) => success
  }

  def waitFor[T](f: Future[T]) = Await.result(f, Duration(30, SECONDS))
}

object CloudSearchSpec extends Specification {

  import TestUtils._

  val domain = ("imdb-movies", "5d3sfdtvri2lghw27klaho756y")

  import CloudSearchParsers._
  import aws.core.parsers._
  import play.api.libs.json._

  case class Movie(id: String, titles: Seq[String])
  implicit val moviesParser = Parser[Seq[Movie]] { r =>
    import play.api.libs.json.util._
    val reader = ((__ \ "id").read[String] and
    (__ \ "data" \ "title").read[Seq[String]])(Movie)

    Success((r.json \ "hits" \ "hit").as[Seq[JsValue]].map { js =>
      js.validate(reader).get
    })
  }

  "CloudSearch API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "Search using String query" in {
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title")))
      checkResult(r)
    }

    "Search using MatchExpression and Filter" in {
      import CloudSearch.MatchExpressions._
      val ex = Field("title", "star wars") and Filter("year", 2008)
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        matchExpression = Some(ex),
        returnFields = Seq("title")))
      checkResult(r)
    }

    "Search using MatchExpression and Filter range" in {
      import CloudSearch.MatchExpressions._
      val ex = Field("title", "star wars") and Filter("year", 2000 to 2012)
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        matchExpression = Some(ex),
        returnFields = Seq("title")))
      checkResult(r)
    }

    "Search using MatchExpression (Field and Not)" in {
      import CloudSearch.MatchExpressions._
      val ex = Field("title", "star wars") and Not(Filter("year", 2000 to 2012))
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        matchExpression = Some(ex),
        returnFields = Seq("title", "year")))
      checkResult(r)
    }

    "Search using MatchExpression (star wars OR star strek)" in {
      import CloudSearch.MatchExpressions._
      val ex = Field("title", "star wars") or Field("title", "star strek")
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        matchExpression = Some(ex),
        returnFields = Seq("title", "year")))
      checkResult(r)
    }

    "Search using MatchExpression (Complex query)" in {
      import CloudSearch.MatchExpressions._
      val ex = (Field("title", "Star Wars") or Field("title", "Star Trek")) and
        Filter("year", 1980 to 1990) and
        Not(Field("director", "Carpenter")) and
        Not(Field("title", "Spock"))

      val expected =   Seq("Star Trek IV: The Voyage Home",
        "Star Trek V: The Final Frontier",
        "Star Trek: The Wrath of Khan",
        "Star Wars: Episode V - The Empire Strikes Back",
        "Star Wars: Episode VI - Return of the Jedi")

      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        matchExpression = Some(ex),
        returnFields = Seq("title", "year")))
      checkResult(r)

      val res = r.body.map(_.titles).flatten
      res must haveSize(expected.size)
      res must containAllOf(expected)
    }

    "Search, 2 results" in {
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title"),
        size = Some(2)))
      checkResult(r)

      val res = r.body
      res must haveSize(2)
    }

    "Search, ignore 2 first results" in {
      val r = waitFor(CloudSearch.search[Seq[Movie]](
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title"),
        start = Some(3)))
      checkResult(r)

      val res = r.body
    }

    "Search, with facets" in {
      val r = waitFor(CloudSearch.search[CloudSearch.WithFacets[Seq[Movie]]](
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title, genre"),
        facets = Seq("genre")))
      checkResult(r)
    }

    "Search, with facets constraints" in {
      val r = waitFor(CloudSearch.search[CloudSearch.WithFacets[Seq[Movie]]](
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title, genre"),
        facets = Seq("genre"),
        facetConstraints = Seq(FacetConstraint("genre", "Action"))))
      checkResult(r)
      val fs = r.body._2
      fs must haveSize(1)
      fs match {
        case Facet("genre", ("Action", _) :: Nil) :: Nil => success
        case _ => failure
      }
    }

    "Search using open intervals in MatchExpression " in {
       import CloudSearch.MatchExpressions._
        val ex = Field("title", "star wars") and Not(Filter("year", to = Some(2000)))
        val r = waitFor(CloudSearch.search[Seq[Movie]](
          domain = domain,
          matchExpression = Some(ex),
          returnFields = Seq("title", "year")))
        checkResult(r)
    }

    "Sort facets by year (descending ordering)" in {
      val r = waitFor(CloudSearch.search[CloudSearch.WithFacets[Seq[Movie]]](
        domain = domain,
        query = Some("star wars"),
        returnFields = Seq("title, genre"),
        facets = Seq("genre"),
        facetSort = Seq(Sort.COUNT("genre"))))
      checkResult(r)
    }

  }
}