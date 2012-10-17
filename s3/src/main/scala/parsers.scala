package aws.s3

import java.text.SimpleDateFormat
import java.util.Date

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

import aws.s3.models._
import aws.s3.S3.HTTPMethods

object S3Parsers {
  import language.postfixOps

  def parseDate(d: String): Date = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'").parse(d)

  implicit def bucketsParser = Parser[Seq[Bucket]] { r =>
    Success((r.xml \\ "Bucket").map { n =>
      Bucket((n \ "Name").text, parseDate((n \ "CreationDate").text))
    })
  }

  implicit def safeResultParser[M <: S3Metadata, T](implicit mp: Parser[M], p: Parser[T]): Parser[Result[M, T]] =
    errorsParser(mp).or(Parser.resultParser(mp, p))

  implicit def s3MetadataParser = Parser[S3Metadata] { r =>
    Success(S3Metadata(
      requestId = r.header("x-amz-request-id").get,
      id2 = r.header("x-amz-id-2").get,
      versionId = r.header("x-amz-version-id"),
      deleteMarker = r.header("x-amz-delete-marker").map(java.lang.Boolean.parseBoolean).getOrElse(false)))
  }

  def errorsParser[M <: S3Metadata](implicit mp: Parser[M]) = mp.flatMap(meta => Parser[Errors[M]] { r =>
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

  implicit def lifecyclesParser = Parser[Seq[LifecycleConf]] { r =>
    import scala.concurrent.util.Duration
    import java.util.concurrent.TimeUnit._

    Success((r.xml \ "Rule").map{ l =>
      LifecycleConf(
        id = (l \ "ID").map(_.text).headOption,
        prefix = (l \ "Prefix").text,
        status = (l \ "Status").map(n => LifecycleConf.Statuses.withName(n.text)).headOption.get,
        lifetime = (l \ "Expiration" \ "Days").map(v => Duration(java.lang.Integer.parseInt(v.text), DAYS)).headOption.get
      )
    })
  }

}
