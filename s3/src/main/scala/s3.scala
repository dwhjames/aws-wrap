package aws.s3

object S3 {

  val ACCESS_KEY_ID = ""
  val SECRET_ACCESS_KEY = ""

  object HTTPMethods extends Enumeration {
    type Method = Value
    val PUT, POST, DELETE, GET = Value
  }
  import HTTPMethods._

  object StorageClasses extends Enumeration {
    type StorageClass = Value
    val STANDARD, REDUCED_REDUNDANCY = Value
  }

  object Parameters {
    import aws.core.AWS._

    def MD5(content: String) = ("Content-MD5" -> aws.core.utils.Crypto.base64(java.security.MessageDigest.getInstance("MD5").digest(content.getBytes)))

    type MimeType = String

    def CacheControl(s: String) = ("Cache-Control" -> s)
    def ContentDisposition(s: String) = ("Content-Disposition" -> s)
    def ContentEncoding(c: java.nio.charset.Charset) = ("Content-Encoding" -> c.name)
    def ContentLength(l: Long) = ("Content-Length" -> l.toString)
    def ContentType(s: MimeType) = ("Content-Type" -> s)
    def Expect(s: MimeType) = ("Expect" -> s)
    def Expires(d: java.util.Date) = {
      val format = new java.text.SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z")
      ("Expires" -> format.format(d))
    }
    def X_AMZ_META(name: String, value: String) = (s"x-amz-meta-$name" -> value)
    def X_AMZ_SERVER_SIDE_ENCRYPTION(s: String) = {
      if(s != "AES256")
        throw new RuntimeException(s"Unsupported server side encoding: $s, the omly valid value is AES256")
      ("x-amz-server-side-encryption" -> s)
    }
    def X_AMZ_STORAGE_CLASS(s: StorageClasses.StorageClass) = ("x-amz-storage-class" -> s.toString)
    def X_AMZ_WEBSITE_REDIRECT_LOCATION(s: java.net.URI) = ("x-amz-website-redirect-location" -> s.toString)


    object Permisions {

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

      object ACLs {
        type ACL = String
        val PRIVATE: ACL = "private"
        val PUBLIC_READ: ACL = "public-read"
        val PUBLIC_READ_WRITE: ACL = "public-read-write"
        val AUTHENTICATED_READ: ACL = "authenticated-read"
        val BUCKET_OWNER_READ: ACL = "bucket-owner_read"
        val BUCKET_OWNER_FULL_CONTROL: ACL = "bucket-owner-full-control"
      }
      import ACLs._
      def X_AMZ_ACL(acl: ACL) = ("x-amz-acl" -> acl)

      import Grantees._
      private def s(gs: Seq[Grantee]) = gs.map { case Grantee(n, v) => """%s="%s"""".format(n, v) }.mkString(", ")

      type Grant = (String, String)
      def GRANT_READ(gs: Grantee*): Grant = "x-amz-grant-read" -> s(gs)
      def GRANT_WRITE(gs: Grantee*): Grant = "x-amz-grant-write" -> s(gs)
      def GRANT_READ_ACP(gs: Grantee*): Grant = "x-amz-grant-read-acp" -> s(gs)
      def GRANT_WRITE_ACP(gs: Grantee*): Grant = "x-amz-grant-write-acp" -> s(gs)
      def GRANT_FULL_CONTROL(gs: Grantee*): Grant = "x-amz-grant-full-control" -> s(gs)
    }
  }

}