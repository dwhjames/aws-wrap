package aws.core.signature

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.parsers._
import aws.core.utils._

case class V2[M <: Metadata](val version: String = "2009-01-15") {

  private val SIGVERSION = "2"
  private val SIGMETHOD = "HmacSHA1"

  protected def request(parameters: Seq[(String, String)])(implicit region: AWSRegion): Future[Response] = {
    WS.url("https://" + region.host + "/?" + signedUrl("GET", parameters)).get()
  }

  private def tryParse[T](resp: Response)(implicit p: Parser[Result[M, T]]) = Parser.parse[Result[M, T]](resp).fold(
    e => throw new RuntimeException(e),
    identity)

  protected def get[T](parameters: (String, String)*)(implicit region: AWSRegion, p: Parser[Result[M, T]]): Future[Result[M, T]] =
    request(parameters).map(tryParse[T])

  protected def signedUrl(method: String, params: Seq[(String, String)])(implicit region: AWSRegion): String = {

    import AWS.Parameters._
    import aws.core.SignerEncoder.encode

    val ps = Seq(
      TimeStamp(new java.util.Date()),
      AWSAccessKeyId(AWS.key),
      Version(version),
      SignatureVersion(SIGVERSION),
      SignatureMethod(SIGMETHOD))

    val queryString = canonicalQueryString(params ++ ps)

    val toSign = "%s\n%s\n%s\n%s".format(method, region.host, "/", queryString)

    "Signature=" + encode(signature(toSign)) + "&" + queryString
  }

  private def signature(data: String) = Crypto.base64(Crypto.hmacSHA1(data.getBytes(), AWS.secret))

}

