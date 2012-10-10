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
  }

  private def ressource(bucketname: String, uri: String, subresource: Option[String] = None) =
    "/%s\n%s\n?%s".format(bucketname, uri, subresource.getOrElse(""))

  private def request(method: Method, bucketname: String, body: String, parameters: (String, String)*)(implicit region: AWSRegion): Future[Response] = {
    val uri = "https://" + bucketname + ".s3.amazonaws.com"
    val res = ressource(bucketname, uri)
    val r = WS.url(uri).withHeaders(S3Sign.sign(method.toString, bucketname): _*)
    method match {
      case PUT => r.put(body)
      case _ => throw new RuntimeException("Unsuported method: " + method)
    }
  }

  private def tryParse[M <: Metadata, T](resp: Response)(implicit p: Parser[Result[M,T]]) = Parser.parse[Result[M,T]](resp).fold(
    e => throw new RuntimeException(e),
    identity
  )

  import Parameters._

  implicit def emptyMetaParser: Parser[EmptyMeta.type] = Parser.pure(EmptyMeta)

  def createBucket(bucketname: String)(implicit region: AWSRegion) = {
      val body =
        <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <LocationConstraint>{region.subdomain}</LocationConstraint>
        </CreateBucketConfiguration>
      request(PUT, bucketname, body.toString).map(tryParse[EmptyMeta.type, Unit])
  }

}

