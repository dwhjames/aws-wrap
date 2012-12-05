/*
* Copyright 2012 Pellucid and Zenexity
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package aws.cloudsearch {

  import java.util.Date
  import java.util.Locale

  import scala.concurrent.duration.Duration
  import scala.concurrent.ExecutionContext.Implicits.global

  import play.api.libs.ws._
  import play.api.libs.ws.WS._
  import play.api.libs.json._

  import aws.core._
  import aws.core.Types._
  import aws.core.parsers._
  import aws.core.utils._

  case class CloudSearchMetadata(requestId: String, time: Duration, cpuTime: Duration) extends Metadata
  case class Facet(name: String, constraints: Seq[(String, Int)])
  case class SDF[T](id: String, version: Int, lang: Locale, document: Option[T])

  object Statuses extends Enumeration {
    type Status = Value
    val SUCCESS = Value("success")
    val ERROR = Value("error")
  }

  case class BatchResponse(status: Statuses.Status, adds: Int, deletes: Int, errors: Seq[String], warnings: Seq[String])

  /**
  * This class represents the field values (facet constraints) that you want to count for a particular field.
  * Constraint can be Strings, bounded or unbounded ranges, and numeric values.
  * {{{
  *    Search(
  *      domain,
  *      query = Some("star wars"),
  *      returnFields = Seq("title"))
  *    .withFacets("genre")
  *    .withFacetConstraints(FacetConstraint("genre", "Action")) // Only count facets for field 'genre', with value 'Action'
  *    .search[CloudSearch.WithFacets[Seq[Movie]]]()
  * }}}
  */
  class FacetConstraint private(val field: String, val value: String)
  object FacetConstraint {
    def apply(field: String, value: Number) = new FacetConstraint(field, s"$value")
    def apply(field: String, value: String) = new FacetConstraint(field, s"'$value'")
    def apply(field: String, range: Range) = new FacetConstraint(field, s"${range.start}..${range.end}")
    def apply(field: String, from: Option[Int] = None, to: Option[Int] = None) = new FacetConstraint(field, s"""${from.getOrElse("")}..${to.getOrElse("")}""")
    def apply(field: String, values: Seq[String]) = {
      val vs = values.map(v => s"'$v'").mkString(",")
      new FacetConstraint(field, s"$vs")
    }
  }


  // TODO: would be nice to check for double '-' calls
  /**
  * How you want to sort facet values for a particular field.
  * Use methods in the Sort object to create instances.
  * {{{
  *   Search(
  *      domain,
  *      query = Some("star wars"),
  *      returnFields = Seq("title"))
  *   .withFacets("genre", "year")
  *   .withFacetSorts(-Sort.Max("year")) // Sort the facet values according to the maximum values in the specified field. (descending order)
  *   .search[Seq[Movie]]()
  * }}}
  */
  sealed trait Sort {
    val field: String
    def value: String
  }

  object Orderings extends Enumeration {
    type Ordering = Value
    val ASC = Value("")
    val DESC = Value("-")
  }

  object Sort {
    /**
    * Sort the facet values alphabetically (in ascending order).
    */
    case class Alpha(field: String) extends Sort {
      override val value = "Alpha"
    }
    /**
    * Sort the facet values by their counts (in descending order).
    */
    case class Count(field: String) extends Sort {
      override val value = "Count"
    }
    /**
    * Sort the facet values according to the maximum values in the specified field.
    * The facet values are sorted in ascending order. To sort in descending order, prefix the sort option with - (minus): -Sort.Max("field").
    */
    case class Max(field: String, ordering: Option[Orderings.Ordering] = None) extends Sort {
      override def value = {
        val order = ordering.map(_.toString).getOrElse("")
        s"${order}Max($field)"
      }
      def unary_- = this.copy(ordering = Some(Orderings.DESC))
    }
    /**
    * Sort the facet values according to the sum of the values in the specified field (in ascending order).
    */
    case class Sum(field: String) extends Sort {
      override def value = s"Sum($field)"
    }
  }

  /**
  *  Fields or rank expression to use for ranking.
  *  A maximum of 10 fields and rank expressions can be specified per query.
  *  You can use any uint field to rank results numerically.
  *  Any result-enabled text or literal field can be used to rank results alphabetically.
  *  To rank results by relevance, you can specify the name of a custom rank expression or text_relevance.
  *  Hits are ordered according to the specified rank field(s).
  *  By default, hits are ranked in ascending order.
  *  You can prefix a field name with a minus (-) to rank in descending order.
  *  If no rank parameter is specified, it defaults to rank=-text_relevance, which lists results according to their text_relevance scores with the highest-scoring documents first.
  *
  * {{{
  *   Search(
  *      domain,
  *      query = Some("star wars"),
  *      returnFields = Seq("title"))
  *   .withRanks(-Rank.Field("year"), -Rank.TextRelevance())
  *   .search[Seq[Movie]]()
  * }}}
  */
  sealed trait Rank {
    val name: String
    val ordering: Option[Orderings.Ordering]
    override def toString = {
      val order = ordering.map(_.toString).getOrElse("")
      s"${order}${name}"
    }
  }
  object Rank {
    /**
    * Rank results by relevance
    */
    case class TextRelevance(ordering: Option[Orderings.Ordering] = None) extends Rank{
      val name = "text_relevance"
      def unary_- = this.copy(ordering = Some(Orderings.DESC))
    }
    /**
    * Rank results alphabetically.
    */
    case class Field(name: String, ordering: Option[Orderings.Ordering] = None) extends Rank {
      def unary_- = this.copy(ordering = Some(Orderings.DESC))
    }
    /**
    * Rank results using a rank expression.
    * {{{
    *   Search(
    *     domain,
    *     query = Some("star wars"),
    *     returnFields = Seq("title"))
    *   .withRanks(-Rank.RankExpr("customExpression", Some("cos(text_relevance)")))
    *   .search[Seq[Movie]]()
    * }}}
    * @see: http://docs.amazonwebservices.com/cloudsearch/latest/developerguide/rankexpressions.html
    */
    case class RankExpr(name: String, expr: Option[String], ordering: Option[Orderings.Ordering] = None) extends Rank {
      def unary_- = this.copy(ordering = Some(Orderings.DESC))
    }

    def weight(ws: (String, Float)*) = {
      import play.api.libs.json._
      Json.obj("weights" -> ws.toMap[String, Float]).toString
    }
  }

  /**
  * Build a match expression that define a Boolean search.
  * {{{
  *   // This expression will match movies with title containing "Star Wars" or "Star Trek"
  *   // realeased from 1980 to 1990, that haven't been directed by "Carpenter", and the title must not contains "Spock".
  *
  *   val ex = (Field("title", "Star Wars") or Field("title", "Star Trek")) and
  *      Filter("year", 1980 to 1990) and
  *      !Field("director", "Carpenter") and
  *      !Field("title", "Spock")
  *
  *   Search(domain)
  *      .withMatchExpression(ex)
  *      .withReturnFields("title", "year")
  *      .search[Seq[Movie]]())
  * }}}
  */
  object MatchExpressions {
    sealed trait MatchExpression {
      def and(ex: MatchExpression) = And(this, ex)
      def or(ex: MatchExpression) = Or(this, ex)
      def unary_! = Not(this)
    }

    class Field private(name: String, value: String) extends MatchExpression {
      override def toString = s"(field $name $value)"
    }
    object Field {
      def apply(name: String, value: Number) = new Field(name, s"$value")
      def apply(name: String, range: Range) = new Field(name, s"${range.start}..${range.end}")
      def apply(name: String, from: Option[Int] = None, to: Option[Int] = None) = new Field(name, s"""${from.getOrElse("")}..${to.getOrElse("")}""")
      def apply(name: String, value: String) = new Field(name, s"'$value'")
    }

    class Filter private(name: String, value: String) extends MatchExpression {
      override def toString = s"(filter $name $value)"
    }
    object Filter {
      def apply(name: String, value: Number) = new Filter(name, s"$value")
      def apply(name: String, range: Range) = new Filter(name, s"${range.start}..${range.end}")
      def apply(name: String, from: Option[Int] = None, to: Option[Int] = None) = new Filter(name, s"""${from.getOrElse("")}..${to.getOrElse("")}""")
      def apply(name: String, value: String) = new Filter(name, s"'$value'")
    }

    class Not(ex: MatchExpression) extends MatchExpression {
      override def toString = s"(not ${ex})"
    }
    object Not {
      def apply(ex: MatchExpression) = new Not(ex)
    }

    class And(ms: Seq[MatchExpression]) extends MatchExpression {
      override def toString = {
        val es = ms.map(_.toString).mkString(" ")
        s"(and ${es})"
      }
    }
    object And {
      def apply(ms: MatchExpression*) = new And(ms)
    }

    class Or(ms: Seq[MatchExpression]) extends MatchExpression {
      override def toString = {
        val es = ms.map(_.toString).mkString(" ")
        s"(or ${es})"
      }
    }
    object Or {
      def apply(ms: MatchExpression*) = new Or(ms)
    }
  }

  object CloudSearch {

    import aws.cloudsearch.CloudSearchParsers._

    val VERSION = "2011-02-01"
    type WithFacets[T] = (T, Seq[Facet])

    private def tryParse[T](resp: Response)(implicit p: Parser[Result[CloudSearchMetadata, T]]) =
      Parser.parse[Result[CloudSearchMetadata, T]](resp).fold(e => throw new RuntimeException(e), identity)

    private def request[T](domain: (String, String), params: Seq[(String, String)] = Nil)(implicit region: CloudSearchRegion, p: Parser[Result[CloudSearchMetadata, T]]) = {

      val allHeaders = Nil

      WS.url(s"http://search-${domain._1}-${domain._2}.${region.subdomain}.cloudsearch.amazonaws.com/${VERSION}/search")
        .withHeaders(allHeaders: _*)
        .withQueryString(params: _*)
        .get()
        .map(tryParse[T])
    }

    def search[T](search: Search)(implicit region: CloudSearchRegion, p: Parser[Result[CloudSearchMetadata, T]]) =
      request[T](search.domain, search.toParams)

    def delete(domain: (String, String), id: String, version: Int)(implicit region: CloudSearchRegion) = {
      import Json._
      val json = Json.arr(Json.obj(
          "type" -> "delete",
          "id" -> id,
          "version" -> version))

      WS.url(s"http://search-${domain._1}-${domain._2}.${region.subdomain}.cloudsearch.amazonaws.com/${VERSION}/documents/batch")
        .post(json)
        .map(resp => Parser.parse[SimpleResult[BatchResponse]](resp).fold(e => throw new RuntimeException(e), identity))
    }

    def upload[T](domain: (String, String), sdf: SDF[T])(implicit w: Writes[T], region: CloudSearchRegion) = {
      val allHeaders = Nil
      val version = "2011-02-01"

      val json = Json.arr(
        Json.obj(
          "type" -> "add",
          "id" -> sdf.id,
          "version" -> sdf.version,
          "lang" -> sdf.lang.getLanguage,
          "fields" -> w.writes(sdf.document.get)))

      WS.url(s"http://search-${domain._1}-${domain._2}.${region.subdomain}.cloudsearch.amazonaws.com/${VERSION}/documents/batch")
        .withHeaders(allHeaders: _*)
        .post(json)
        .map(resp => Parser.parse[SimpleResult[BatchResponse]](resp).fold(e => throw new RuntimeException(e), identity))
    }
  }

  case class Search(domain: (String, String),
    query: Option[String] = None,
    matchExpression: Option[MatchExpressions.MatchExpression] = None,
    returnFields: Seq[String] = Nil,
    facets: Seq[String] = Nil,
    facetConstraints: Seq[FacetConstraint] = Nil,
    facetSort: Seq[Sort] = Nil,
    facetTops: Seq[(String, Int)] = Nil,
    ranks: Seq[Rank] = Nil,
    scores: Seq[(String, Range)] = Nil,
    size: Option[Int] = None,
    startAt: Option[Int] = None) {
      def withMatchExpression(m: MatchExpressions.MatchExpression) = this.copy(matchExpression = Some(m))
      def withReturnFields(fs: String*) = this.copy(returnFields = this.returnFields ++ fs)
      def withFacets(fs: String*) = this.copy(facets = this.facets ++ fs)
      def withFacetConstraints(fs: FacetConstraint*) = this.copy(facetConstraints = this.facetConstraints ++ fs)
      def withFacetSorts(fs: Sort*) = this.copy(facetSort = this.facetSort ++ fs)
      def withFacetTops(fs: (String, Int)*) = this.copy(facetTops = this.facetTops ++ fs)
      def withRanks(fs: Rank*) = this.copy(ranks = this.ranks ++ fs)
      def withScores(ss: (String, Range)*) = this.copy(scores = this.scores ++ ss)
      def withSize(s: Int) = this.copy(size = Some(s))
      def startAt(i: Int) = this.copy(startAt = Some(i))

      /**
      * Execute query, and parse results as type T
      * You must provide an implicit Parser[T]
      * For example, to search for movies:
      * {{{
      *   // parse movies out of a response
      *   implicit val moviesParser = Parser[Seq[Movie]] { r =>
      *     import play.api.libs.json.util._
      *     val reader = ((__ \ "id").read[String] and
      *     (__ \ "data" \ "title").read[Seq[String]])(Movie)
      *
      *     Success((r.json \ "hits" \ "hit").as[Seq[JsValue]].map { js =>
      *       js.validate(reader).get
      *     })
      *   }
      *
      *   val movies: Result[CloudSearchMetadata, Seq[Movie]] = Search(
      *     domain = domain,
      *     query = Some("star wars"),
      *     returnFields = Seq("title"))
      *   .search[Seq[Movie]]()
      * }}}
      *
      * Note: you can get facets using "CloudSearch.WithFacets[T]".
      * It's a tuple containing the expected result type, and facets: (T, Seq[Facet])
      * {{{
      *   type MoviesWithFacets = CloudSearch.WithFacets[Seq[Movie]] // CloudSearch.WithFacets[Seq[Movie]] === (Seq[Movie], Seq[Facet])
      *   val movies: Result[CloudSearchMetadata, Seq[Movie]] = Search(
      *     domain = domain,
      *     query = Some("star wars"),
      *     returnFields = Seq("title"))
      *   .search[MoviesWithFacets]()
      * }}}
      */
      def search[T]()(implicit region: CloudSearchRegion, p: Parser[Result[CloudSearchMetadata, T]]) =
        CloudSearch.search[T](this)

      def toParams = {
        val exprs = ranks.flatMap {
          case e: Rank.RankExpr =>
            e.expr.map(x => s"facet-${e.name}-sort" -> x)
          case _ => Nil
        }

        query.toSeq.map("q" -> _) ++
        returnFields.reduceLeftOption(_ + "," + _).map("return-fields" -> _).toSeq ++
        matchExpression.map("bq" -> _.toString).toSeq ++
        facets.reduceLeftOption(_ + "," + _).map("facet" -> _).toSeq ++
        size.map("size" -> _.toString).toSeq ++
        startAt.map("start" -> _.toString).toSeq ++
        facetConstraints.map(c => s"facet-${c.field}-constraints" -> c.value) ++
        facetSort.map(f => s"facet-${f.field}-sort" -> f.value) ++
        facetTops.map(t => s"facet-${t._1}-top-n" -> t._2.toString) ++
        ranks.map(_.toString).reduceLeftOption(_ + "," + _).map("rank" -> _).toSeq ++
        exprs ++
        scores.map(s => s"t-${s._1}" -> s"${s._2.start}..${s._2.end}")
      }
  }

}

/**
* Java API
*/
package com.pellucid.aws.cloudsearch {
  import scala.concurrent.ExecutionContext.Implicits.global

  import aws.core._
  import aws.core.parsers.Parser
  import aws.cloudsearch._
  import com.pellucid.aws.cloudsearch.models.{Search => JSearch}

  object JavaConverters {
    import scala.collection.JavaConverters._
    def fromJava(js: JSearch) = aws.cloudsearch.Search(
        js.getDomain.getName -> js.getDomain.getId,
        Option(js.getQuery),
        //Option(js.getMatchExpression.toScala),
        returnFields = js.getReturnFields.asScala
        //js.getFacets,
        //js.getFacetConstraints,
        //js.getFacetSort,
        //js.getFacetTops,
        //js.getRanks,
        //js.getScores,
        //js.getSize,
        //js.getStartAt
      )
  }

  class CloudSearch(val scalaRegion: CloudSearchRegion) {
    import JavaConverters._
    import scala.concurrent.Future
    import com.pellucid.aws.results.{ Result => JResult }

    def this() = this(CloudSearchRegion.DEFAULT)

    def search[T](search: JSearch, p: Parser[T]): Future[JResult[CloudSearchMetadata, T]] = {
      import aws.core.parsers._
      import CloudSearchParsers._

      implicit val parser = p
      implicit val region = scalaRegion
      fromJava(search).search[T].map { result =>
        new JResult[CloudSearchMetadata, T] {
          def metadata = result.metadata
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = result.body
        }
      }
    }
  }
}
