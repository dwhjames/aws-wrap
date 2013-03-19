package aws.s3

object Permissions {

  object Grantees {
    sealed class Grantee(n: String, v: String) {
      val name = n
      val value = v
    }
    object Grantee {
      def apply(name: String, value: String) = new Grantee(name, value)
      def unapply(g: Grantee): Option[(String, String)] = Some((g.name, g.value))
    }
    case class Email(override val value: String) extends Grantee("emailAddress", value)
    case class Id(override val value: String) extends Grantee("id", value)
    case class Uri(override val value: String) extends Grantee("uri", value)
  }

    
  import ACLs._
  def X_AMZ_ACL(acl: ACL) = ("x-amz-acl" -> acl)

  import Grantees._
  private def s(gs: Seq[Grantee]) = gs.map { case Grantee(n, v) => "%s=\"%s\"".format(n, v) }.mkString(", ")

  type Grant = (String, String)
  def GRANT_READ(gs: Grantee*): Grant = "x-amz-grant-read" -> s(gs)
  def GRANT_WRITE(gs: Grantee*): Grant = "x-amz-grant-write" -> s(gs)
  def GRANT_READ_ACP(gs: Grantee*): Grant = "x-amz-grant-read-acp" -> s(gs)
  def GRANT_WRITE_ACP(gs: Grantee*): Grant = "x-amz-grant-write-acp" -> s(gs)
  def GRANT_FULL_CONTROL(gs: Grantee*): Grant = "x-amz-grant-full-control" -> s(gs)
}