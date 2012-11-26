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

package aws.cloudsearch

import java.util.Date

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

import play.api.libs.ws._
import play.api.libs.ws.WS._

import aws.core._
import aws.core.Types._
import aws.core.parsers._
import aws.core.utils._

case class CloudSearchMetadata(requestId: String, time: Duration, cpuTime: Duration) extends Metadata
case class Facet(name: String, constraints: Seq[(String, Int)])

class FacetConstraint private(val field: String, val value: String)
object FacetConstraint {
  def apply(field: String, value: Number) = new FacetConstraint(field, s"$value")
  def apply(field: String, value: String) = new FacetConstraint(field, s"'$value'")
  def apply(field: String, range: Range) = new FacetConstraint(field, s"${range.start}..${range.end}")
  def apply(field: String, values: Seq[String]) = {
    val vs = values.map(v => s"'$v'").mkString(",")
    new FacetConstraint(field, s"$vs")
  }
}


case class Sort private(field: String, value: String){
  def unary_- = Sort(this.field, s"-${this.value}")
}
object Sort {
  def ALPHA(field: String) = Sort(field, "Alpha")
  def COUNT(field: String) = Sort(field, "Count")
  def MAX(field: String) = Sort(field, s"Max($field)")
  def SUM(field: String) = Sort(field, s"Sum($field)")
}


object CloudSearch {

  type WithFacets[T] = (T, Seq[Facet])

  object Parameters {

  }

  private def tryParse[T](resp: Response)(implicit p: Parser[Result[CloudSearchMetadata, T]]) =
    Parser.parse[Result[CloudSearchMetadata, T]](resp).fold(e => throw new RuntimeException(e), identity)

  private def request[T](domain: (String, String), params: Seq[(String, String)] = Nil)(implicit region: CloudSearchRegion, p: Parser[Result[CloudSearchMetadata, T]]) = {

    val allHeaders = Nil
    val version = "2011-02-01"

    WS.url(s"http://search-${domain._1}-${domain._2}.${region.subdomain}.cloudsearch.amazonaws.com/${version}/search")
      .withHeaders(allHeaders: _*)
      .withQueryString(params: _*)
      .get()
      .map(tryParse[T])
  }

  // TODO: open range
  object MatchExpressions {
    sealed trait MatchExpression {
      def and(ex: MatchExpression) = And(this, ex)
      def or(ex: MatchExpression) = Or(this, ex)
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

  def search[T](
    domain: (String, String),
    query: Option[String] = None,
    matchExpression: Option[MatchExpressions.MatchExpression] = None,
    returnFields: Seq[String] = Nil,
    facets: Seq[String] = Nil,
    facetConstraints: Seq[FacetConstraint] = Nil,
    facetSort: Seq[Sort] = Nil,
    size: Option[Int] = None,
    start: Option[Int] = None)(implicit region: CloudSearchRegion, p: Parser[Result[CloudSearchMetadata, T]]) = {

    val params =
      query.toSeq.map("q" -> _) ++
      returnFields.reduceLeftOption(_ + "," + _).map("return-fields" -> _).toSeq ++
      matchExpression.map("bq" -> _.toString).toSeq ++
      facets.reduceLeftOption(_ + "," + _).map("facet" -> _).toSeq ++
      size.map("size" -> _.toString).toSeq ++
      start.map("start" -> _.toString).toSeq ++
      facetConstraints.map(c => s"facet-${c.field}-constraints" -> c.value) ++
      facetSort.map(f => s"facet-${f.field}-sort" -> f.value)

    request[T](domain, params)
  }

}
