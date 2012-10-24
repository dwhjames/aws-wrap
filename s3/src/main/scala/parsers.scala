package aws.s3

import java.text.SimpleDateFormat
import java.util.Date

import java.lang.{ Boolean => JBool, Integer => JInt, Long => JLong }

import scala.xml.Node

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

  implicit def s3MetadataParser = Parser[S3Metadata] { r =>
    Success(S3Metadata(
      requestId = r.header("x-amz-request-id").get,
      id2 = r.header("x-amz-id-2").get,
      versionId = r.header("x-amz-version-id"),
      deleteMarker = r.header("x-amz-delete-marker").map(JBool.parseBoolean).getOrElse(false)))
  }

  def errorsParser = s3MetadataParser.flatMap(meta => Parser[AWSError[S3Metadata]] { r =>
    (r.status match {
      // TODO: really test content (some errors come with a 200)
      case s if (s < 300) => Some(Failure("Error expected, found success (status 2xx)"))
      case _ => for (
        code <- (r.xml \\ "Error" \ "Code").headOption.map(_.text);
        message <- (r.xml \\ "Error" \ "Message").headOption.map(_.text)
      ) yield Success(AWSError(meta, code, message))
    }).getOrElse(sys.error("Failed to parse error: " + r.body))
  })

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[S3Metadata, T]] =
    errorsParser.or(Parser.resultParser(s3MetadataParser, p))

  implicit def loggingStatusParser = Parser[Seq[LoggingStatus]] { r =>
    Success((r.xml \\ "LoggingEnabled").map { n =>
      val grants = (n \ "TargetGrants" \ "Grant").toSeq.map { g =>
        val mail = (g \ "Grantee" \ "EmailAddress").text
        val perm = (g \ "Permission").text
        S3.Parameters.Permisions.Grantees.Email(mail) -> perm
      }
      LoggingStatus((n \ "TargetBucket").text, (n \ "TargetPrefix").text, grants)
    })
  }

  implicit def tagsParser = Parser[Seq[Tag]] { r =>
    Success((r.xml \\ "Tag").map { t =>
      Tag((t \ "Key").text, (t \ "Value").text)
    })
  }

  implicit def corsRulesParser = Parser[Seq[CORSRule]] { r =>
    Success((r.xml \\ "CORSRule").map { c =>
      CORSRule(
        origins = (c \ "AllowedOrigin").map(_.text),
        methods = (c \ "AllowedMethod").map(n => HTTPMethods.withName(n.text)),
        headers = (c \ "AllowedHeader").map(_.text),
        maxAge = (c \ "MaxAgeSeconds").map(l => JLong.parseLong(l.text)).headOption,
        exposeHeaders = (c \ "ExposeHeader").map(_.text))
    })
  }

  implicit def lifecyclesParser = Parser[Seq[LifecycleConf]] { r =>
    import scala.concurrent.util.Duration
    import java.util.concurrent.TimeUnit._

    Success((r.xml \ "Rule").map { l =>
      LifecycleConf(
        id = (l \ "ID").map(_.text).headOption,
        prefix = (l \ "Prefix").text,
        status = (l \ "Status").map(n => LifecycleConf.Statuses.withName(n.text)).headOption.get,
        lifetime = (l \ "Expiration" \ "Days").map(v => Duration(java.lang.Integer.parseInt(v.text), DAYS)).headOption.get)
    })
  }

  //XXX: not really a Parser
  import S3Object.StorageClasses
  private def containerParser[T](node: Node, f: (Option[String], String, Boolean, Date, String, Long, StorageClasses.StorageClass, Owner) => T): T = {
    f((node \ "VersionId").map(_.text).headOption,
      (node \ "Key").text,
      (node \ "IsLatest").map(n => JBool.parseBoolean(n.text)).headOption.get,
      // TODO: date format
      (node \ "LastModified").map(n => new Date()).headOption.get,
      (node \ "ETag").text,
      (node \ "Size").map(n => JLong.parseLong(n.text)).headOption.get,
      (node \ "StorageClass").map(n => StorageClasses.withName(n.text)).headOption.get,
      (node \ "Owner").map(ownerParser).headOption.get)
  }

  private def ownerParser(node: Node): Owner = {
    Owner(
      id = (node \ "ID").text,
      name = (node \ "DisplayName").map(_.text).headOption)
  }

  implicit def versionsParser = Parser[Versions] { r =>
    val xml = r.xml
    Success(Versions(
      name = (xml \ "Name").text,
      prefix = (xml \ "Prefix").filter(!_.text.isEmpty).map(_.text).headOption,
      key = (xml \ "KeyMarker").filter(!_.text.isEmpty).map(_.text).headOption,
      versionId = (xml \ "VersionIdMarker").filter(!_.text.isEmpty).map(_.text).headOption,
      maxKeys = (xml \ "MaxKeys").headOption.map(n => JLong.parseLong(n.text)).get,
      isTruncated = (xml \ "IsTruncated").headOption.map(n => JBool.parseBoolean(n.text)).get,
      versions = (xml \ "Version").map(containerParser(_, Version.apply)),
      deleteMarkers = (xml \ "DeleteMarker").map(containerParser(_, DeleteMarker.apply))))
  }

  implicit def s3ObjectParser = Parser[S3Object] { r =>
    val xml = r.xml
    Success(S3Object(
      name = (xml \ "Name").text,
      prefix = (xml \ "Prefix").headOption.filter(!_.text.isEmpty).map(_.text),
      marker = (xml \ "Marker").headOption.filter(!_.text.isEmpty).map(_.text),
      maxKeys = (xml \ "MaxKeys").headOption.map(n => JLong.parseLong(n.text)).get,
      isTruncated = (xml \ "IsTruncated").headOption.map(n => JBool.parseBoolean(n.text)).get,
      contents = (xml \ "Contents").map(containerParser(_, Content.apply))))
  }

  implicit def policyParser = Parser[Policy] { r =>
    import aws.s3.JsonFormats._
    Success(r.json.as[Policy])
  }

}
