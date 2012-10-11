package aws.s3

import java.util.Date

import scala.concurrent.Future
import scala.xml.Elem
import scala.concurrent.ExecutionContext.Implicits.global

import play.api.libs.ws._
import play.api.libs.ws.WS._

import aws.core._
import aws.core.Types._
import aws.core.parsers._

import aws.s3.signature._
import aws.s3.S3Parsers._

object S3 {

  val ACCESS_KEY_ID = ""
  val SECRET_ACCESS_KEY = ""

  object HTTPMethods extends Enumeration {
    type Method = Value
    val PUT, POST, DELETE, GET = Value
  }
  import HTTPMethods._

  object Parameters {
    import AWS._

    object Permisions {

      object Grantees {
        sealed case class Grantee(name: String, value: String)
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
      private def s(gs: Seq[Grantee]) = gs.map{ case Grantee(n,v) => """%s="%s"""".format(n,v) }.mkString(", ")

      type Grant = (String, String)
      def GRANT_READ(gs: Grantee*): Grant = "x-amz-grant-read" -> s(gs)
      def GRANT_WRITE(gs: Grantee*): Grant = "x-amz-grant-write" -> s(gs)
      def GRANT_READ_ACP(gs: Grantee*): Grant = "x-amz-grant-read-acp" -> s(gs)
      def GRANT_WRITE_ACP(gs: Grantee*): Grant = "x-amz-grant-write-acp" -> s(gs)
      def GRANT_FULL_CONTROL(gs: Grantee*): Grant = "x-amz-grant-full-control" -> s(gs)
    }
  }

  private def ressource(bucketname: String, uri: String, subresource: Option[String] = None) =
    "/%s\n%s\n?%s".format(bucketname, uri, subresource.getOrElse(""))

  private def request(method: Method, bucketname: String, body: Option[String] = None, parameters: Seq[(String, String)] = Nil)(implicit region: AWSRegion): Future[Response] = {
    val uri = "https://" + bucketname + ".s3.amazonaws.com"
    val res = ressource(bucketname, uri)
    // TODO: do not hardcode contentType
    val r = WS.url(uri)
      .withHeaders(S3Sign.sign(method.toString, bucketname, contentType = body.map(_ => "text/plain; charset=utf-8")): _*)

    method match {
      case PUT => r.put(body.get)
      case DELETE => r.delete()
      case _ => throw new RuntimeException("Unsuported method: " + method)
    }
  }

  private def tryParse[T](resp: Response)(implicit p: Parser[SimpleResult[T]]) = Parser.parse[SimpleResult[T]](resp).fold(
    e => throw new RuntimeException(e),
    identity)

  import Parameters._
  import Permisions._
  import ACLs._
  import Grantees._

  def createBucket(bucketname: String, acls: Option[ACL] = None, permissions: Seq[Grant] = Nil)(implicit region: AWSRegion): Future[EmptySimpleResult] = {
    val body =
      <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>{ region.subdomain }</LocationConstraint>
      </CreateBucketConfiguration>

    val ps = acls.map(X_AMZ_ACL(_)).toSeq ++ permissions
    request(PUT, bucketname, Some(body.toString), ps).map(tryParse[Unit])
  }

  def deleteBucket(bucketname: String)(implicit region: AWSRegion): Future[EmptySimpleResult] =
    request(DELETE, bucketname).map(tryParse[Unit])

}