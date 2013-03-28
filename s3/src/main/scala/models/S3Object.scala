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
package models

import java.lang.{Boolean => JBool, Long => JLong }
import java.util.Date

import scala.xml.Node

import aws.core.parsers.{Parser, Success}

case class Owner(
  id:   String,
  name: Option[String]
)

case class S3Object(
  name:        String,
  prefix:      Option[String],
  marker:      Option[String],
  maxKeys:     Long,
  isTruncated: Boolean,
  contents:    Seq[Content]
)

object S3Object {

  implicit def s3ObjectParser = Parser[S3Object] { r =>
    val xml = r.xml
    Success(
      S3Object(
        name        = (xml \ "Name").text,
        prefix      = (xml \ "Prefix").headOption.filter(!_.text.isEmpty).map(_.text),
        marker      = (xml \ "Marker").headOption.filter(!_.text.isEmpty).map(_.text),
        maxKeys     = (xml \ "MaxKeys").headOption.map(n => JLong.parseLong(n.text)).get,
        isTruncated = (xml \ "IsTruncated").headOption.map(n => JBool.parseBoolean(n.text)).get,
        contents    = (xml \ "Contents").map(Container.parser(_, Content.apply))
      )
    )
  }
}

trait Container {
  val id:           Option[String]
  val key:          String
  val isLatest:     Boolean
  val lastModified: Date
  val etag:         String
  val size:         Option[Long]
  val storageClass: Option[StorageClass.Value]
  val owner:        Owner
}

object Container {

  private def ownerParser(node: Node): Owner = {
    Owner(
      id   = (node \ "ID").text,
      name = (node \ "DisplayName").map(_.text).headOption
    )
  }

  def parser[T](
    node: Node,
    f: (Option[String],
        String,
        Boolean,
        Date,
        String,
        Option[Long],
        Option[StorageClass.Value],
        Owner
       ) => T
  ): T =
    f(
      (node \ "VersionId").map(_.text).headOption,
      (node \ "Key").text,
      (node \ "IsLatest").map(n => JBool.parseBoolean(n.text)).headOption.get,
      (node \ "LastModified").map(n => new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(n.text)).headOption.get,
      (node \ "ETag").text,
      (node \ "Size").map(n => JLong.parseLong(n.text)).headOption,
      (node \ "StorageClass").map(n => StorageClass.withName(n.text)).headOption,
      (node \ "Owner").map(ownerParser).headOption.get
    )
}

case class DeleteMarker(
  id:           Option[String],
  key:          String,
  isLatest:     Boolean,
  lastModified: Date,
  etag:         String,
  size:         Option[Long],
  storageClass: Option[StorageClass.Value] = Some(StorageClass.STANDARD),
  owner:        Owner
) extends Container

case class Version(
  id:           Option[String],
  key:          String,
  isLatest:     Boolean,
  lastModified: Date,
  etag:         String,
  size:         Option[Long],
  storageClass: Option[StorageClass.Value] = Some(StorageClass.STANDARD),
  owner:        Owner
) extends Container

// TODO: Add content
case class Versions(
  name:          String,
  prefix:        Option[String],
  key:           Option[String],
  maxKeys:       Long,
  isTruncated:   Boolean,
  versions:      Seq[Version],
  deleteMarkers: Seq[DeleteMarker],
  versionId:     Option[String]
)

object Versions {

  implicit def versionsParser = Parser[Versions] { r =>
    val xml = r.xml
    Success(
      Versions(
        name          = (xml \ "Name").text,
        prefix        = (xml \ "Prefix").filter(!_.text.isEmpty).map(_.text).headOption,
        key           = (xml \ "KeyMarker").filter(!_.text.isEmpty).map(_.text).headOption,
        versionId     = (xml \ "VersionIdMarker").filter(!_.text.isEmpty).map(_.text).headOption,
        maxKeys       = (xml \ "MaxKeys").headOption.map(n => JLong.parseLong(n.text)).get,
        isTruncated   = (xml \ "IsTruncated").headOption.map(n => JBool.parseBoolean(n.text)).get,
        versions      = (xml \ "Version").map(Container.parser(_, Version.apply)),
        deleteMarkers = (xml \ "DeleteMarker").map(Container.parser(_, DeleteMarker.apply))
      )
    )
  }
}

case class Content(
  id:           Option[String],
  key:          String,
  isLatest:     Boolean,
  lastModified: Date,
  etag:         String,
  size:         Option[Long],
  storageClass: Option[StorageClass.Value] = Some(StorageClass.STANDARD),
  owner:        Owner
) extends Container

case class BatchDeletion(
  successes: Seq[BatchDeletion.DeletionSuccess],
  failures:  Seq[BatchDeletion.DeletionFailure]
)

object BatchDeletion {

  implicit def deletionsParser = Parser[BatchDeletion] { r =>
    val successes = (r.xml \ "Deleted") map { s =>
      DeletionSuccess(
        (s \ "Key").text,
        (s \ "VersionId").headOption.map(_.text),
        (s \ "DeleteMarker").headOption.map(_.text),
        (s \ "DeleteMarkerVersionId").headOption.map(_.text)
      )
    }
    val failures = (r.xml \ "Error") map { e =>
       DeletionFailure(
        (e \ "Key").text,
        (e \ "Code").text,
        (e \ "Message").text
      )
    }
    Success(BatchDeletion(successes, failures))
  }

  case class DeletionSuccess(
    key:                   String,
    versionId:             Option[String],
    deleteMarker:          Option[String],
    deleteMarkerVersionId: Option[String]
  )

  case class DeletionFailure(
    key:     String,
    code:    String,
    message: String
  )

}
