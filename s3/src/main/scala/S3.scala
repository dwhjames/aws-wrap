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

import aws.s3.models.BucketServiceImpl


object AWS extends  aws.core.AWS{}

object ACLs {
  type ACL = String
  val PRIVATE: ACL = "private"
  val PUBLIC_READ: ACL = "public-read"
  val PUBLIC_READ_WRITE: ACL = "public-read-write"
  val AUTHENTICATED_READ: ACL = "authenticated-read"
  val BUCKET_OWNER_READ: ACL = "bucket-owner_read"
  val BUCKET_OWNER_FULL_CONTROL: ACL = "bucket-owner-full-control"
}

object S3 {

  object Bucket extends BucketServiceImpl

  val ACCESS_KEY_ID = ""
  val SECRET_ACCESS_KEY = ""

  case class MFA(serial: String, token: String)

  object HTTPMethods extends Enumeration {
    type Method = Value
    val PUT, POST, DELETE, GET = Value
  }
  import HTTPMethods._

  object StorageClasses extends Enumeration {
    type StorageClass = Value
    val STANDARD, REDUCED_REDUNDANCY = Value
  }

  object VersionStates extends Enumeration {
    type VersionState = Value
    val ENABLED = Value("Enabled")
    val SUSPENDED = Value("Suspended")
  }

  object MFADeleteStates extends Enumeration {
    type MFADeleteState = Value
    val DISABLED = Value("Disabled")
    val ENABLED = Value("Enabled")
  }

  object Parameters {
    import AWS._

    def MD5(content: String) = ("Content-MD5" -> aws.core.utils.Crypto.base64(java.security.MessageDigest.getInstance("MD5").digest(content.getBytes)))

    def X_AMZ_META(name: String, value: String) = (s"x-amz-meta-$name" -> value)
    def X_AMZ_SERVER_SIDE_ENCRYPTION(s: String) = {
      if(s != "AES256")
        throw new RuntimeException(s"Unsupported server side encoding: $s, the omly valid value is AES256")
      ("x-amz-server-side-encryption" -> s)
    }
    def X_AMZ_STORAGE_CLASS(s: StorageClasses.StorageClass) = ("x-amz-storage-class" -> s.toString)
    def X_AMZ_WEBSITE_REDIRECT_LOCATION(s: java.net.URI) = ("x-amz-website-redirect-location" -> s.toString)
    def X_AMZ_MFA(mfa: MFA) = ("x-amz-mfa" -> s"${mfa.serial} ${mfa.token}")

  }

}
