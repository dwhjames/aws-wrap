package aws.core.signature

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.utils._

object V2 {

  val VERSION = "2009-04-15"
  val SIGVERSION = "2"
  val SIGMETHOD = "HmacSHA1"

  def request(method: String, parameters: Seq[(String, String)])(implicit region: AWSRegion): Future[Response] = {
    WS.url("https://" + region.host + "/?" + V2.signedUrl(method, parameters)).get()
  }

  def signedUrl(method: String, params: Seq[(String, String)])(implicit region: AWSRegion): String = {

    import AWS.Parameters._
    import aws.core.SignerEncoder.encode

    val ps = Seq(
      Expires(600L),
      AWSAccessKeyId(AWS.key),
      Version(VERSION),
      SignatureVersion(SIGVERSION),
      SignatureMethod(SIGMETHOD))

    val queryString = canonicalQueryString(params ++ ps)

    val toSign = "%s\n%s\n%s\n%s".format(method, region.host, "/", queryString)

    "Signature=" + encode(signature(toSign)) + "&" + queryString
  }

  private def signature(data: String) = Crypto.base64(Crypto.hmacSHA1(data.getBytes(), AWS.secret))

}

