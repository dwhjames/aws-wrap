/**
* Java API
*/
package com.pellucid.aws.cloudsearch {
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.collection.JavaConverters._

  import aws.core._
  import aws.core.parsers.Parser
  import aws.cloudsearch._
  import com.pellucid.aws.cloudsearch.models.{ Search => JSearch }

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
        //js.getRanks,
        //js.getScores,
        size = Option(js.size).map(x => x:Int),
        startAt = Option(js.startAt).map(x => x:Int)
      )

    import aws.cloudsearch.MatchExpressions.MatchExpression
    import com.pellucid.aws.cloudsearch.models.{ MatchExpression => JMatchExpression, FacetConstraint => JFacetConstraint, Facet => JFacet, Sort => JSort }
    def toJava(m: MatchExpression): JMatchExpression = new JMatchExpression {
      override val underlying = m
      override def toString = m.toString
    }
    def toJava(f: Facet): JFacet = new JFacet(f.name, f.constraints.toMap.asJava)

    def toScala(m: JMatchExpression): MatchExpression = m.underlying
    def toScala(f: JFacetConstraint): FacetConstraint = f.underlying
    def toScala(s: JSort): Sort = s.underlying

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
      toScala(search).search[T].map { result =>
        new JResult[CloudSearchMetadata, T] {
          def metadata = result.metadata
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = result.body
        }
      }
    }

    import com.pellucid.aws.cloudsearch.models.{ WithFacets => JWithFacets }
    def searchWithFacets[T](search: JSearch, p: Parser[T]): Future[JResult[CloudSearchMetadata, JWithFacets[T]]] = {
      import aws.core.parsers._
      import CloudSearchParsers._

      implicit val parser = p
      implicit val region = scalaRegion

      toScala(search).search[CloudSearch.WithFacets[T]].map { result =>
        new JResult[CloudSearchMetadata, JWithFacets[T]] {
          def metadata = result.metadata
          def isSuccess = result.toEither.fold(err => false, b => true)
          def body = new JWithFacets(result.body._2.map(toJava).asJava, result.body._1)
        }
      }
    }
  }
}



package com.pellucid.aws.cloudsearch.models {

  import scala.reflect.BeanInfo
  import java.util.{ List => JList, Map => JMap }

  import com.pellucid.aws.cloudsearch.JavaConverters._
  import scala.collection.JavaConverters._

  @BeanInfo
  class Domain(val name: String, val id: String){
    override def toString = s"Domain($name, $id)"
  }
  @BeanInfo
  class WithFacets[T](val facets: JList[Facet], val body: T) {
    override def toString = s"WithFacets($facets, $body)"
  }
  @BeanInfo
  class Facet(val name: String, val constraints: JMap[String, Int]) {
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

  // TODO
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

  // TODO
  trait Rank

  @BeanInfo
  class Search(
    val domain: Domain,
    val query: String,
    val matchExpression: MatchExpression,
    val returnFields: JList[String],
    val facets: JList[String],
    val facetConstraints: JList[FacetConstraint],
    val facetSorts: JList[Sort],
    val facetTops: JMap[String, Integer],
    val ranks: JList[Rank],
    val scores: JMap[String, Range],
    val size: Integer,
    val startAt: Integer){

    def this(domain: Domain) = this(domain, null, null, null, null, null, null, null, null, null, null, null)

    def withQuery(query: String) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    def withMatchExpression(matchExpression: MatchExpression) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    // TODO: merge instead of replacing
    @scala.annotation.varargs
    def withReturnFields(returnFields: String*) =
      new Search(domain, query, matchExpression, returnFields.asJava, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withFacets(facets: String*) =
      new Search(domain, query, matchExpression, returnFields, facets.asJava, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withFacetConstraints(facetConstraints: FacetConstraint*) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints.asJava, facetSorts, facetTops, ranks, scores, size, startAt)

    @scala.annotation.varargs
    def withFacetSorts(facetSorts: Sort*) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts.asJava, facetTops, ranks, scores, size, startAt)

    def withFacetTops(facetTops: JMap[String, Integer]) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    // def withRanks(fs: Rank*)
    // def withScores(ss: (String, Range)*)

    def withSize(size: Integer) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    def startAt(startAt: Integer) =
      new Search(domain, query, matchExpression, returnFields, facets, facetConstraints, facetSorts, facetTops, ranks, scores, size, startAt)

    override def toString =
      s"Search($domain, $query, $matchExpression, $returnFields, $facets, $facetConstraints, $facetSorts, $facetTops, $ranks, $scores, $size, $startAt)"
  }
}