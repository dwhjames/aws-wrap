package aws.core.parsers

import scala.annotation.implicitNotFound
import scala.xml.Elem
import scala.util.{ Try, Success, Failure }

import play.api.libs.ws.Response

import aws.core._

@implicitNotFound(
  "No parser found for type ${To}. Try to implement an implicit aws.core.parsers.Parser for this type."
)
trait Parser[To] extends (Response => Try[To])

case class AWSError(code: String, message: String)
case class Errors(errors: Seq[AWSError]) extends RuntimeException

object Parser {

  def apply[To](transformer: (Response => Try[To])): Parser[To] = new Parser[To] {
    def apply(r: Response) = transformer(r)
  }

  def of[To](implicit extractor: Parser[To]) = extractor

  def HandleError[To](p: Response => Try[To]) = new Parser[To]{
    def apply(r: Response) = r.status match {
      case 200 => p(r)
      case _ => errorsParser(r).transform(e => Failure(Errors(e)), Failure(_))
    }
  }

  implicit def metadataParser = Parser[Metadata] { r =>
    Success(Metadata(r.xml \\ "RequestId" text, r.xml \\ "BoxUsage" text))
  }

  implicit def errorsParser = Parser[Seq[AWSError]] { r =>
    Success((r.xml \\ "Error").map { node =>
      AWSError(node \ "Code" text, node \ "Message" text)
    })
  }

}
