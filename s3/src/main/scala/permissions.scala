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

object Permissions {

  sealed trait Grantee {
    val name:  String
    val value: String
  }

  case class Email(override val value: String) extends Grantee { override val name = "emailAddress" }
  case class Id   (override val value: String) extends Grantee { override val name = "id" }
  case class Uri  (override val value: String) extends Grantee { override val name = "uri" }

  def X_AMZ_ACL(acl: CannedACL.Value) =
    ("x-amz-acl" -> acl.toString)

  private def s(gs: Seq[Grantee]) =
    gs map { g =>
      "%s=\"%s\"".format(g.name, g.value)
    } mkString (", ")

  type Grant = (String, String)
  def GRANT_READ        (gs: Grantee*): Grant = "x-amz-grant-read"         -> s(gs)
  def GRANT_WRITE       (gs: Grantee*): Grant = "x-amz-grant-write"        -> s(gs)
  def GRANT_READ_ACP    (gs: Grantee*): Grant = "x-amz-grant-read-acp"     -> s(gs)
  def GRANT_WRITE_ACP   (gs: Grantee*): Grant = "x-amz-grant-write-acp"    -> s(gs)
  def GRANT_FULL_CONTROL(gs: Grantee*): Grant = "x-amz-grant-full-control" -> s(gs)
}
