package aws.s3

object Permissions {

  object Grantees {

    sealed trait Grantee {
      val name:  String
      val value: String
    }

    case class Email(override val value: String) extends Grantee { override val name = "emailAddress" }
    case class Id   (override val value: String) extends Grantee { override val name = "id" }
    case class Uri  (override val value: String) extends Grantee { override val name = "uri" }
  }

  def X_AMZ_ACL(acl: CannedACL.Value) =
    ("x-amz-acl" -> acl.toString)

  import Grantees._
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
