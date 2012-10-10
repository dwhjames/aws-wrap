package aws.dynamodb

import play.api.libs.json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.core._
import aws.core.parsers._
import aws.dynamodb.models._

object DDBParsers {

  implicit val emptyMetadataParser: Parser[EmptyMeta.type] = Parser.pure(EmptyMeta)

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

  implicit def safeResultParser[M <: Metadata, T](implicit mp: Parser[M], p: Parser[T]): Parser[Result[M, T]] =
    jsonErrorsParser.or(Parser.resultParser(mp ,p))

  def jsonErrorsParser[M <: Metadata](implicit mp: Parser[M]) = mp.flatMap(meta => Parser[Errors[M]] { r =>
    r.status match {
      // TODO: really test content
      case 200 => Failure("Not an error")
      case _ => Success(Errors(meta, Seq(r.json.as[AWSError])))
    }
  })

  implicit val AWSErrorFormat = (
    (__ \ '__type).format[String] and
    (__ \ 'message).format[String]
  )(AWSError, unlift(AWSError.unapply))

}

