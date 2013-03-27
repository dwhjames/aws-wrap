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

object HttpMethod extends Enumeration {
  val PUT, POST, DELETE, GET = Value
}

object StorageClass extends Enumeration {
  val STANDARD, REDUCED_REDUNDANCY = Value
}

object VersionState extends Enumeration {
  val ENABLED   = Value("Enabled")
  val SUSPENDED = Value("Suspended")
}

object MFADeleteState extends Enumeration {
  val DISABLED = Value("Disabled")
  val ENABLED  = Value("Enabled")
}

object LifecycleStatus extends Enumeration {
  val ENABLED  = Value("Enabled")
  val DISABLED = Value("Disabled")
}

object LoggingPermission extends Enumeration {
  val FULL_CONTROL, READ, WRITE = Value
}

object NotificationEvent extends Enumeration {
  val REDUCED_REDUNDANCY_LOST_OBJECT = Value("s3:ReducedRedundancyLostObject")
}

object PolicyEffect extends Enumeration {
  val ALLOW = Value("Allow")
  val DENY  = Value("Deny")
}

object CannedACL extends Enumeration {
  val PRIVATE                   = Value("private")
  val PUBLIC_READ               = Value("public-read")
  val PUBLIC_READ_WRITE         = Value("public-read-write")
  val AUTHENTICATED_READ        = Value("authenticated-read")
  val BUCKET_OWNER_READ         = Value("bucket-owner_read")
  val BUCKET_OWNER_FULL_CONTROL = Value("bucket-owner-full-control")
}

object LocationConstraint extends Enumeration {
  val US_WEST_1      = Value("us-west-1")
  val US_WEST_2      = Value("us-west-2")
  val EU_WEST_1      = Value("eu-west-1")
  val SA_EAST_1      = Value("sa-east-1")
  val AP_SOUTHEAST_1 = Value("ap-southeast-1")
  val AP_SOUTHEAST_2 = Value("ap-southeast-2")
  val AP_NORTHEAST_1 = Value("ap-northeast-1")
  val US_STANDARD    = Value("")

  implicit val DEFAULT = US_STANDARD
}
