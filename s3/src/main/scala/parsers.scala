package aws.s3

import java.text.SimpleDateFormat
import java.util.Date

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

import aws.s3.models._
import aws.s3.S3.HTTPMethods

object S3Parsers {

  def parseDate(d: String): Date = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'").parse(d)

  implicit def bucketsParser = Parser[Seq[Bucket]] { r =>
    Success((r.xml \\ "Bucket").map { n =>
      Bucket((n \ "Name").text, parseDate((n \ "CreationDate").text))
    })
  }

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

  implicit def loggingStatusParser = Parser[Seq[LoggingStatus]] { r =>
    Success( (r.xml \\ "LoggingEnabled").map { n =>
      val grants = (n \ "TargetGrants" \ "Grant").toSeq.map { g =>
          val mail = (g \ "Grantee" \ "EmailAddress" ).text
          val perm = (g \ "Permission").text
          S3.Parameters.Permisions.Grantees.Email(mail) -> perm
        }
      LoggingStatus((n \ "TargetBucket").text, (n \ "TargetPrefix").text, grants)
    })
  }

  implicit def tagsParser = Parser[Seq[Tag]] { r =>
    Success( (r.xml \\ "Tag").map { t =>
      Tag((t \ "Key").text, (t \ "Value").text)
    })
  }

  implicit def corsRulesParser = Parser[Seq[CORSRule]] { r =>
    Success( (r.xml \\ "CORSRule").map { c =>
      CORSRule(
        origins = (c \ "AllowedOrigin").map(_.text),
        methods = (c \ "AllowedMethod").map(n => HTTPMethods.withName(n.text)),
        headers = (c \ "AllowedHeader").map(_.text),
        maxAge  = (c \ "MaxAgeSeconds").map(l => java.lang.Long.parseLong(l.text)).headOption,
        exposeHeaders = (c \ "ExposeHeader").map(_.text))
    })
  }

}
