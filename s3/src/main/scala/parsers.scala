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

package aws.s3

import java.text.SimpleDateFormat
import java.util.Date

import java.lang.{ Boolean => JBool, Integer => JInt, Long => JLong }

import scala.xml.Node

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

import aws.s3.models._

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

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[S3Metadata, T]] =
    Parser.xmlErrorParser[S3Metadata].or(Parser.resultParser(s3MetadataParser, p))

  implicit def loggingStatusParser = Parser[Seq[LoggingStatus]] { r =>
    Success((r.xml \\ "LoggingEnabled").map { n =>
      val grants = (n \ "TargetGrants" \ "Grant").toSeq.map { g =>
        val mail = (g \ "Grantee" \ "EmailAddress").text
        val perm = (g \ "Permission").text
        aws.s3.Permissions.Grantees.Email(mail) -> LoggingPermission.withName(perm)
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
        methods = (c \ "AllowedMethod").map(n => HttpMethod.withName(n.text)),
        headers = (c \ "AllowedHeader").map(_.text),
        maxAge = (c \ "MaxAgeSeconds").map(l => JLong.parseLong(l.text)).headOption,
        exposeHeaders = (c \ "ExposeHeader").map(_.text))
    })
  }

  implicit def lifecyclesParser = Parser[Seq[LifecycleConf]] { r =>
    import scala.concurrent.duration.Duration
    import java.util.concurrent.TimeUnit._

    Success((r.xml \ "Rule").map { l =>
      LifecycleConf(
        id = (l \ "ID").map(_.text).headOption,
        prefix = (l \ "Prefix").text,
        status = (l \ "Status").map(n => LifecycleStatus.withName(n.text)).headOption.get,
        lifetime = (l \ "Expiration" \ "Days").map(v => Duration(java.lang.Integer.parseInt(v.text), DAYS)).headOption.get)
    })
  }

  //XXX: not really a Parser
  private def containerParser[T](node: Node, f: (Option[String], String, Boolean, Date, String, Option[Long], Option[StorageClass.Value], Owner) => T): T = {
    f((node \ "VersionId").map(_.text).headOption,
      (node \ "Key").text,
      (node \ "IsLatest").map(n => JBool.parseBoolean(n.text)).headOption.get,
      (node \ "LastModified").map(n => new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(n.text)).headOption.get,
      (node \ "ETag").text,
      (node \ "Size").map(n => JLong.parseLong(n.text)).headOption,
      (node \ "StorageClass").map(n => StorageClass.withName(n.text)).headOption,
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

  implicit def deletionsParser = Parser[BatchDeletion] { r =>

    val successes = (r.xml \ "Deleted").map{ s =>
      BatchDeletion.DeletionSuccess(
        (s \ "Key").text,
        (s \ "VersionId").headOption.map(_.text),
        (s \ "DeleteMarker").headOption.map(_.text),
        (s \ "DeleteMarkerVersionId").headOption.map(_.text))
    }

    val failures = (r.xml \ "Error").map{ e =>
       BatchDeletion.DeletionFailure(
          (e \ "Key").text,
          (e \ "Code").text,
          (e \ "Message").text)
    }

    Success(BatchDeletion(successes, failures))
  }

  implicit def notificationParser = Parser[Seq[NotificationConfiguration]] { r =>
    Success(
      (r.xml \ "TopicConfiguration").map{ t =>
        NotificationConfiguration(
          (t \ "Topic").text,
          NotificationEvent.withName((t \ "Event").text))
      })
  }

}
