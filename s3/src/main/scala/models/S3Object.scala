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

import java.util.Date

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
