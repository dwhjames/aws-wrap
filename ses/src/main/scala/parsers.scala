package aws.ses

import scala.xml.Node

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object SESParsers {

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SESMetadata, T]] =
    Parser.xmlErrorParser[SESMetadata].or(Parser.resultParser(sesMetadataParser, p))

  implicit def emailResultParser = Parser[EmailResult] { r =>
    Success(EmailResult((r.xml \\ "MessageId").text))
  }

  implicit def sesMetadataParser = Parser[SESMetadata] { r =>
    Success(SESMetadata((r.xml \\ "RequestId").text))
  }
}
