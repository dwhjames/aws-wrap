package aws.s3

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object S3Parsers {

  implicit def safeResultParser[M <: Metadata, T](implicit mp: Parser[M], p: Parser[T]): Parser[Result[M, T]] =
    errorsParser.or(Parser.resultParser(mp, p))

  def errorsParser[M <: Metadata](implicit mp: Parser[M]) = mp.flatMap(meta => Parser[Errors[M]] { r =>
    r.status match {
      // TODO: really test content
      case s if (s < 300) => Failure("Error expected, found success (status 2xx)")
      case _ => Success(Errors(meta, (r.xml \\ "Error").map { node =>
        AWSError(node \ "Code" text, node \ "Message" text)
      }))
    }
  })

}
