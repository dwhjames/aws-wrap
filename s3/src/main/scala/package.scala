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

package aws

import core.Metadata
import core.utils.Crypto

import java.net.URI

/**
 * Access to Amazon [[http://aws.amazon.com/documentation/s3 S3]].
 *
 * Amazon Simple Storage Service (Amazon S3) is a web service that enables you to store data in the cloud.
 *
 */
package object s3 {

  case class MFA(
    serial: String,
    token: String
  )

  case class S3Metadata(
    requestId:    String,
    id2:          String,
    versionId:    Option[String] = None,
    deleteMarker: Boolean        = false
  ) extends Metadata

  object Parameters {

    def MD5(content: String) =
      (
        "Content-MD5" ->
        Crypto.base64(java.security.MessageDigest.getInstance("MD5").digest(content.getBytes))
      )

    def X_AMZ_META(name: String, value: String) =
      (s"x-amz-meta-$name" -> value)

    def X_AMZ_SERVER_SIDE_ENCRYPTION(s: String) = {
      if(s != "AES256")
        throw new RuntimeException(s"Unsupported server side encoding: $s, the only valid value is AES256")
      ("x-amz-server-side-encryption" -> s)
    }

    def X_AMZ_STORAGE_CLASS(s: StorageClass.Value) =
      ("x-amz-storage-class" -> s.toString)

    def X_AMZ_WEBSITE_REDIRECT_LOCATION(s: URI) =
      ("x-amz-website-redirect-location" -> s.toString)

    def X_AMZ_MFA(mfa: MFA) =
      ("x-amz-mfa" -> s"${mfa.serial} ${mfa.token}")

  }

}
