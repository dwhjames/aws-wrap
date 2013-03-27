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

import Permissions.Grantees.Grantee

import aws.core.parsers.{Parser, Success}

case class LoggingStatus(
  bucket: String,
  prefix: String,
  grants: Seq[(Grantee, LoggingPermission.Value)]
)

object LoggingStatus {

  implicit def loggingStatusParser = Parser[Seq[LoggingStatus]] { r =>
    Success((r.xml \\ "LoggingEnabled") map { n =>
      val grants = (n \ "TargetGrants" \ "Grant").toSeq map { g =>
        val mail = (g \ "Grantee" \ "EmailAddress").text
        val perm = (g \ "Permission").text
        Permissions.Grantees.Email(mail) -> LoggingPermission.withName(perm)
      }
      LoggingStatus(
        (n \ "TargetBucket").text,
        (n \ "TargetPrefix").text,
        grants
      )
    })
  }
}
