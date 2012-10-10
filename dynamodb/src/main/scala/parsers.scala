package aws.dynamodb

import play.api.libs.json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.core._
import aws.core.parsers._
import aws.dynamodb.models._

object DDBParsers {

  // TODO: Use a type (we may need Seq[String] for something else)
  implicit def tableListParser = Parser[Seq[String]] { r =>
    Success((r.json \ "TableNames").as[Seq[String]])
  }

  implicit def tableDescriptionParser = Parser[TableDescription] { r =>
    ((r.json \ "TableDescription"), (r.json \ "Table")) match {
      case (desc: JsObject, _) => Success(desc.as[TableDescription])
      case (_, desc: JsObject) => Success(desc.as[TableDescription])
      case _ => Failure("Error parsing Json response: " + r.body)
    }
  }

  // Generic parsers

  implicit def jsonResultParser[T](implicit fjs: Reads[T]) = Parser[T] { r =>
    r.json.validate[T] match {
      case JsSuccess(result, _) => Success(result)
      case JsError(errors) => Failure(errors.toString)
    }
  }

  implicit def safeResultParser[M <: Metadata, T](implicit mp: Parser[M], p: Parser[T]): Parser[Result[M, T]] =
    jsonErrorsParser.or(Parser.resultParser(mp, p))

  def jsonErrorsParser[M <: Metadata](implicit mp: Parser[M]) = mp.flatMap(meta => Parser[Errors[M]] { r =>
    r.status match {
      case 200 => Failure("Not an error")
      case _ => Success(Errors(meta, Seq(r.json.as[AWSError])))
    }
  })

  implicit val AWSErrorFormat = Reads[AWSError](js => (js \ "__type", js \ "Message", js \ "message") match {
    case (JsString(t), JsString(m), _) => JsSuccess(AWSError(t, m))
    case (JsString(t), _, JsString(m)) => JsSuccess(AWSError(t, m))
    case _ => JsError("JsObject expected")
  })

}

