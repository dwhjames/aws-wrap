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

object CloudSearch {

  object Parameters {

  }

  private def tryParse[T](resp: Response)(implicit p: Parser[Result[CloudSearchMetadata, T]]) =
    Parser.parse[Result[CloudSearchMetadata, T]](resp).fold(e => throw new RuntimeException(e), identity)

  private def request[T](domain: (String, String), params: Seq[(String, String)] = Nil)(implicit region: CloudSearchRegion/*, p: Parser[Result[CloudSearchMetadata, T]]*/) = {

    val allHeaders = Nil
    val version = "2011-02-01"

    WS.url(s"http://search-${domain._1}-${domain._2}.${region.subdomain}.cloudsearch.amazonaws.com/${version}/search")
      .withHeaders(allHeaders: _*)
      .withQueryString(params: _*)
      .get()
      //.map(tryParse[T])
  }

  // TODO
  sealed trait MatchExpression {
    def and(ex: MatchExpression) = And(this, ex)
    def or(ex: MatchExpression) = Or(this, ex)
    val | = or _
    val & = or _
  }
  case class Field(name: String, value: String) extends MatchExpression {
    // TODO: open-ended range
    def this(name: String, range: Range) = this(name, s"${range.start}..${range.end}")
    override def toString = s"(field $name '$value')"
  }
  object Field {
    def apply(name: String, range: Range) = new Field(name, range)
  }
  case class Filter(name: String, value: String) extends MatchExpression {
    def this(name: String, range: Range) = this(name, s"${range.start}..${range.end}")
    override def toString = s"(filter $name '$value')"
  }
  object Filter {
    def apply(name: String, range: Range) = new Filter(name, range)
  }
  case class Not(ex: MatchExpression) extends MatchExpression {
    override def toString = s"(not ${ex})"
  }
  case class And(ms: MatchExpression*) extends MatchExpression {
    override def toString = {
      val es = ms.map(_.toString).mkString(" ")
      s"(and ${es})"
    }
  }
  case class Or(ms: MatchExpression*) extends MatchExpression {
    override def toString = {
      val es = ms.map(_.toString).mkString(" ")
      s"(or ${es})"
    }
  }

  def search(
    domain: (String, String),
    query: Option[String] = None,
    matchExpression: Option[MatchExpression] = None,
    returnFields: Seq[String] = Nil) = {

    val params =
      query.toSeq.map("q" -> _) ++
      returnFields.reduceLeftOption(_ + "," + _).map("return-fields" -> _).toSeq
    request(domain, params)
  }

}
