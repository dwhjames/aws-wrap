/**
* Java API
*/
package com.pellucid.aws.cloudsearch {
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.collection.JavaConverters._

  import aws.core._
  import aws.core.parsers.Parser
  import aws.cloudsearch._
  import com.pellucid.aws.cloudsearch.models.{
    Search => JSearch,
    MatchExpression => JMatchExpression,
    FacetConstraint => JFacetConstraint,
    Facet => JFacet,
    Sort => JSort,
    Rank => JRank,
    Score,
    SDF => JSDF,
    Domain,
    BatchResponse => JBatchResponse,
    CloudSearchMetadata => JCloudSearchMetadata }

  object JavaConverters {

    def toScala(js: JSearch): Search = aws.cloudsearch.Search(
        js.domain.name -> js.domain.id,
        Option(js.query),
        Option(js.matchExpression).map(toScala),
        js.returnFields.asScala,
        Option(js.facets).map(_.asScala).toSeq.flatten,
        Option(js.facetConstraints).map(_.asScala).toSeq.flatten.map(toScala),
        Option(js.facetSorts).map(_.asScala).toSeq.flatten.map(toScala),
        Option(js.facetTops).map(_.asScala.mapValues(x => x: Int).toSeq).toSeq.flatten,
        Option(js.ranks).map(_.asScala).toSeq.flatten.map(toScala),
        Option(js.scores).map(_.asScala).toSeq.flatten.map(toScala),
        Option(js.size).map(x => x:Int),
        Option(js.startAt).map(x => x:Int)
      )

    import aws.cloudsearch.MatchExpressions.MatchExpression
    def toJava(m: MatchExpression): JMatchExpression = new JMatchExpression {
      override val underlying = m
      override def toString = m.toString
    }
    def toJava(f: Facet): JFacet = new JFacet(f.name, f.constraints.toMap.asJava)
    def toJava(b: BatchResponse): JBatchResponse = new JBatchResponse(
      b.status.toString,
      b.adds,
      b.deletes,
      Option(b.errors).toSeq.flatten.asJava,
      Option(b.warnings).toSeq.flatten.asJava)

    def toJava(c: CloudSearchMetadata): JCloudSearchMetadata =
      new JCloudSearchMetadata(c.requestId, c.time.toMillis, c.cpuTime.toMillis)

    def toScala(m: JMatchExpression): MatchExpression = m.underlying
    def toScala(f: JFacetConstraint): FacetConstraint = f.underlying
    def toScala(s: JSort): Sort = s.underlying
    def toScala(r: JRank): Rank = r.underlying
    def toScala(s: Score): (String, Range) = s.underlying
    def toScala(s: JSDF): SDF[org.codehaus.jackson.JsonNode] = SDF(s.id, s.version, s.lang, Option(s.document))
    def toScala(s: Domain): (String, String) = s.name -> s.id
  }

  class CloudSearch(val scalaRegion: CloudSearchRegion) {
    import JavaConverters._
    import scala.concurrent.Future
    import com.pellucid.aws.results.{ Result => JResult, SimpleResult => JSimpleResult }

    def this() = this(CloudSearchRegion.DEFAULT)

    def search[T](search: JSearch, p: Parser[T]): Future[JResult[JCloudSearchMetadata, T]] = {
      import aws.core.parsers._
      import CloudSearchParsers._

      implicit val parser = p
      implicit val region = scalaRegion
      toScala(search).search[T].map { result =>
        new JResult[JCloudSearchMetadata, T] {
          def metadata = toJava(result.metadata)
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = result.body
        }
      }
    }

    import com.pellucid.aws.cloudsearch.models.{ WithFacets => JWithFacets }
    def searchWithFacets[T](search: JSearch, p: Parser[T]): Future[JResult[JCloudSearchMetadata, JWithFacets[T]]] = {
      import aws.core.parsers._
      import CloudSearchParsers._

      implicit val parser = p
      implicit val region = scalaRegion

      toScala(search).search[CloudSearch.WithFacets[T]].map { result =>
        new JResult[JCloudSearchMetadata, JWithFacets[T]] {
          def metadata = toJava(result.metadata)
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = new JWithFacets(result.body._2.map(toJava).asJava, result.body._1)
        }
      }
    }

    import com.pellucid.aws.cloudsearch.models.{ Domain, SDF => JSDF }
    def upload(domain: Domain, doc: JSDF): Future[JSimpleResult[JBatchResponse]] = {
      import org.codehaus.jackson._
      import org.codehaus.jackson.map._

      implicit val region = scalaRegion

      val mapper = new ObjectMapper

      val content = mapper.createObjectNode
      content.put("type", "add")
      content.put("id", doc.id)
      content.put("version", doc.version)
      content.put("lang", doc.lang.getLanguage)
      content.put("fields", doc.document)

      val json = mapper.createArrayNode
      json.add(content)


      CloudSearch.uploadRaw(toScala(domain), json.toString).map { result =>
        new JSimpleResult[JBatchResponse] {
          def metadata = throw new RuntimeException("SimpleResult does not have metadata")
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = toJava(result.body)
        }
      }
    }

    def delete(domain: Domain, id: String, version: Int): Future[JSimpleResult[JBatchResponse]] = {
      implicit val region = scalaRegion
      CloudSearch.delete(toScala(domain), id, version).map { result =>
        new JSimpleResult[JBatchResponse] {
          def metadata = throw new RuntimeException("SimpleResult does not have metadata")
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = toJava(result.body)
        }
      }
    }

  }
}

package com.pellucid.aws.cloudsearch.models {

  import java.util.{ List => JList, Map => JMap, Locale }
  import scala.reflect.BeanProperty
  import com.pellucid.aws.cloudsearch.JavaConverters._
  import scala.collection.JavaConverters._


  class CloudSearchMetadata(@BeanProperty val requestId: String, @BeanProperty val time: Long, @BeanProperty val cpuTime: Long)

  class BatchResponse(@BeanProperty val status: String, @BeanProperty val adds: Int, @BeanProperty val deletes: Int, @BeanProperty val errors: JList[String], @BeanProperty val warnings: JList[String])

  class SDF(@BeanProperty val id: String, @BeanProperty val version: Int, @BeanProperty val lang: Locale, @BeanProperty val document: org.codehaus.jackson.JsonNode) {
    override def toString = s"SDF($id, $version, $lang, $document)"
  }

  class Score(@BeanProperty val underlying: (String, Range))
  object Score {
    def range(name: String, from: Int, to: Int) = new Score(name -> Range(from, to))
  }


  class Domain(@BeanProperty val name: String, @BeanProperty val id: String){
    override def toString = s"Domain($name, $id)"
  }

  class WithFacets[T](@BeanProperty val facets: JList[Facet], @BeanProperty val body: T) {
    override def toString = s"WithFacets($facets, $body)"
  }

  class Facet(@BeanProperty val name: String, @BeanProperty val constraints: JMap[String, Int]) {
    override def toString = s"Facet($name, $constraints)"
  }

  abstract class MatchExpression {
    val underlying: aws.cloudsearch.MatchExpressions.MatchExpression
    def and(m: MatchExpression) = toJava(toScala(this).and(toScala(m)))
    def or(m: MatchExpression) = toJava(toScala(this).or(toScala(m)))
  }
  object MatchExpression {
    import aws.cloudsearch.MatchExpressions.{ MatchExpression => _, _ }
    def field(name: String, value: String) = toJava(Field(name, value))
    def filterValue(name: String, value: Int) = toJava(Filter(name, value))
    def filterValue(name: String, value: String) = toJava(Filter(name, value))
    def filterRange(name: String, from: Int, to: Int) = toJava(Filter(name, scala.Range(from, to)))
    def filterTo(name: String, to: Int) = toJava(Filter(name, to = Some(to)))
    def filterFrom(name: String, from: Int) = toJava(Filter(name, from = Some(from)))
    def not(m: MatchExpression) = toJava(Not(toScala(m)))
  }

  import aws.cloudsearch.{ FacetConstraint => FC }
  class FacetConstraint(val underlying: FC) {
    def this(field: String, value: Number) = this(FC.apply(field, value))
    def this(field: String, value: String) = this(FC.apply(field, value))
    def this(field: String, from: Integer, to: Integer) = this(FC.apply(field, Option(from), Option(to)))
    def this(field: String, values: JList[String]) = this(FC.apply(field, values.asScala))
  }

  import aws.cloudsearch.{ Sort => SSort }
  trait Ordering
  object Desc extends Ordering
  object Asc extends Ordering
  object Order {
    val DESC = Desc
    val ASC = Asc
  }
  class Sort(val underlying: SSort)
  class Ordered(override val underlying: SSort, val order: Ordering) extends Sort(underlying)

  object Sort {
    def alpha(field: String) = new Sort(SSort.Alpha(field))
    def count(field: String) = new Sort(SSort.Count(field))
    def max(field: String) = new Sort(SSort.Max(field))
    def max(field: String, ordering: Ordering) = ordering match {
      case o: Desc.type => new Sort(-SSort.Max(field))
      case _ => new Sort(SSort.Max(field))
    }
    def sum(field: String) = new Sort(SSort.Sum(field))
  }

  import aws.cloudsearch.{ Orderings, Rank => SRank }
  class Rank(val underlying: SRank)
  object Rank {
    import SRank._
    def textRelevance() = new Rank(TextRelevance())
    def textRelevance(ordering: Ordering) = ordering match {
      case o: Desc.type => new Rank(TextRelevance(Some(Orderings.DESC)))
      case _ => new Rank(TextRelevance(Some(Orderings.ASC)))
    }
    def field(name: String) = new Rank(Field(name))
    def field(name: String, ordering: Ordering) = ordering match {
      case o: Desc.type => new Rank(Field(name, Some(Orderings.DESC)))
      case _ => new Rank(Field(name, Some(Orderings.ASC)))
    }

    def rankExpr(name: String) = new Rank(RankExpr(name, None))
    def rankExpr(name: String, expr: String) = new Rank(RankExpr(name, Some(expr)))
    def rankExpr(name: String, expr: String, ordering: Ordering) = ordering match {
      case o: Desc.type => new Rank(RankExpr(name, Some(expr), Some(Orderings.DESC)))
      case _ => new Rank(RankExpr(name, Some(expr), Some(Orderings.ASC)))
    }
  }

  class Search(
    @BeanProperty val domain: Domain,
    @BeanProperty val query: String,
    @BeanProperty val matchExpression: MatchExpression,
    @BeanProperty val returnFields: JList[String],
    @BeanProperty val facets: JList[String],
    @BeanProperty val facetConstraints: JList[FacetConstraint],
    @BeanProperty val facetSorts: JList[Sort],
    @BeanProperty val facetTops: JMap[String, Integer],
    @BeanProperty val ranks: JList[Rank],
    @BeanProperty val scores: JList[Score],
    @BeanProperty val size: Integer,
    @BeanProperty val startAt: Integer){

    private def concat[T](l1: JList[T], l2: Seq[T]): JList[T] = (l1.asScala ++ l2).asJava

    def this(domain: Domain) = this(domain, null, null, null, null, null, null, null, null, null, null, null)

    def withQuery(query: String) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    def withMatchExpression(matchExpression: MatchExpression) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withReturnFields(returnFields: String*) =
      new Search(domain, query, matchExpression, concat(this.returnFields, returnFields), facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withFacets(facets: String*) =
      new Search(domain, query, matchExpression, returnFields, concat(this.facets, facets), facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withFacetConstraints(facetConstraints: FacetConstraint*) =
      new Search(domain, query, matchExpression, returnFields, facets, concat(this.facetConstraints, facetConstraints), facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withFacetSorts(facetSorts: Sort*) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, concat(this.facetSorts, facetSorts), facetTops, ranks, scores, size, startAt)

    // TODO: merge instead of replacing
    def withFacetTops(facetTops: JMap[String, Integer]) = {
      val merged  = (facetTops.asScala ++ this.facetTops.asScala).asJava
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, merged, ranks, scores, size, startAt)
    }

    @scala.annotation.varargs
    def withRanks(ranks: Rank*) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, concat(this.ranks, ranks), scores, size, startAt)

    @scala.annotation.varargs
    def withScores(scores: Score*) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, concat(this.scores, scores), size, startAt)

    def withSize(size: Integer) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    def startAt(startAt: Integer) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    override def toString =
      s"Search($domain, $query, $matchExpression, $returnFields, $facets, $facetConstraints, $facetSorts, $facetTops, $ranks, $scores, $size, $startAt)"
  }
}