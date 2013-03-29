package aws.cloudsearch

import java.lang.{ Long => JLong, Double => JDouble, Boolean => JBool }

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit._

import play.api.libs.ws.Response
import play.api.libs.json.util._
import play.api.libs.json._
import play.api.libs.functional.syntax._

import aws.core._
import aws.core.parsers._

object CloudSearchParsers {

  implicit def batchResponseParser = Parser[BatchResponse] { r =>
    val read = r.json.validate((
      (__ \ "status").read[String].map(Statuses.withName(_)) and
      (__ \ "adds").read[Int] and
      (__ \ "deletes").read[Int] and
      (__ \ "errors").read[Seq[JsObject]].map(_.map(x => (x \ "message").as[String])).orElse(Reads.pure(Nil)) and
      (__ \ "warnings").read[Seq[JsObject]].map(_.map(x => (x \ "message").as[String])).orElse(Reads.pure(Nil)))(BatchResponse))
    Success(read.get)
  }

  implicit def cloudSearchMetadataParser = Parser[CloudSearchMetadata] { r =>
    val read = (r.json \ "info").validate((
      (__ \ "rid").read[String] and
      (__ \ "time-ms").read[Long].map(Duration(_, MILLISECONDS)) and
      (__ \ "cpu-time-ms").read[Long].map(Duration(_, MILLISECONDS)))(CloudSearchMetadata))
    Success(read.get)
  }

  //XXX: refactor
  implicit def facetsParser = Parser[Seq[Facet]] { r =>
    val facets = (r.json \ "facets").validate[JsObject]
    val jsresult = facets.flatMap { f =>
      val results = f.fields.map {
        case (k, co) =>
          val constraints = co \ "constraints"
          constraints.validate[Seq[(String, Int)]](Reads.seq(
            ((__ \ "value").read[String] and (__ \ "count").read[Int]).tupled)).map(Facet(k, _))
      }

      results.foldLeft(JsSuccess(Nil): JsResult[Seq[Facet]]) {
        case (JsSuccess(seq, _), n) =>
          n match {
            case JsSuccess(e, _) => JsSuccess(seq :+ e)
            case err: JsError => err
          }
        case (e: JsError, _) => e
      }
    }

    Success(jsresult.get)
  }

  implicit def entityWithFacetsParser[T](implicit ep: Parser[T], fsp: Parser[Seq[Facet]]): Parser[(T, Seq[Facet])] =
    ep and fsp

  implicit def safeSimpleResultParser[T](implicit p: Parser[T]): Parser[SimpleResult[T]] = {
    val resultParser = Parser.resultParser(Parser.emptyMetadataParser, p)
    Parser.xmlErrorParser[EmptyMeta.type].or(resultParser)
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[CloudSearchMetadata, T]] =
    Parser.xmlErrorParser[CloudSearchMetadata].or(Parser.resultParser(cloudSearchMetadataParser, p))

}
