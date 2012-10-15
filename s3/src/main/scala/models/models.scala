package aws.s3.models

import java.util.Date

import play.api.libs.ws._

import scala.concurrent.Future
import scala.xml.Elem

import aws.core._
import aws.core.Types._
import aws.core.parsers.Parser

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

private[models] object Http {

  import aws.s3.signature.S3Sign

  def ressource(bucketname: Option[String], uri: String, subresource: Option[String] = None) =
    "/%s\n%s\n?%s".format(bucketname.getOrElse(""), uri, subresource.getOrElse(""))

  def request(method: Method, bucketname: Option[String] = None, subresource: Option[String] = None, body: Option[String] = None, parameters: Seq[(String, String)] = Nil): Future[Response] = {

    val uri = bucketname.map("https://" + _ + ".s3.amazonaws.com").getOrElse("https://s3.amazonaws.com") + subresource.map("/?" + _).getOrElse("")
    val res = ressource(bucketname, uri)
    // TODO: do not hardcode contentType
    val r = WS.url(uri)
      .withHeaders(parameters ++ S3Sign.sign(method.toString, bucketname, subresource, contentType = body.map(_ => "text/plain; charset=utf-8"), headers = parameters): _*)

    method match {
      case PUT => r.put(body.get)
      case DELETE => r.delete()
      case GET => r.get()
      case _ => throw new RuntimeException("Unsuported method: " + method)
    }
  }

  def tryParse[T](resp: Response)(implicit p: Parser[SimpleResult[T]]) =
    Parser.parse[SimpleResult[T]](resp).fold( e => throw new RuntimeException(e), identity)

}

case class Bucket(name: String, creationDate: Date)
object Bucket {
  import Http._
  import Parameters._
  import Permisions._
  import ACLs._
  import Grantees._

  case class LoggingStatus(bucket: String, prefix: String, grants: Seq[(Grantee, String)])
  object Logging {

    def enable(loggedBucket: String, targetBucket: String, grantees: Seq[Grantee] = Nil) = {
      val body = 
        <BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01">
          <LoggingEnabled>
            <TargetBucket>{ targetBucket.toLowerCase }</TargetBucket>
            <TargetPrefix>{ loggedBucket.toLowerCase }-access_log-/</TargetPrefix>
            <TargetGrants>
              <Grant>
                <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">
                  <EmailAddress>julien.tournay@pellucid.com</EmailAddress>
                </Grantee>
                <Permission>FULL_CONTROL</Permission>
              </Grant>
            </TargetGrants>
          </LoggingEnabled>
        </BucketLoggingStatus>

      val fr = request(PUT, Some(loggedBucket), body = Some(body.toString), subresource = Some("logging"))
      fr.map(tryParse[Unit])
    }

    def get(bucketName: String) = 
      request(GET, Some(bucketName), subresource = Some("logging")).map(tryParse[Seq[LoggingStatus]])
  }

  def create(bucketname: String, acls: Option[ACL] = None, permissions: Seq[Grant] = Nil)(implicit region: AWSRegion): Future[EmptySimpleResult] = {
    val body =
      <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>{ region.subdomain }</LocationConstraint>
      </CreateBucketConfiguration>

    val ps = acls.map(X_AMZ_ACL(_)).toSeq ++ permissions
    request(PUT, Some(bucketname), body = Some(body.toString), parameters = ps).map(tryParse[Unit])
  }

  def delete(bucketname: String): Future[EmptySimpleResult] =
    request(DELETE, Some(bucketname)).map(tryParse[Unit])

  def list(): Future[SimpleResult[Seq[Bucket]]] =
    request(GET).map(tryParse[Seq[Bucket]])
    
  // TODO:
  // cors (put / delete)
  // logging
  // notifications
  // tagging
  // versionning
  // website
  // requestPayment
  // location
  // lifecycle
  // policy
}