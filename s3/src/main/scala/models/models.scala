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

import aws.s3.S3.Parameters.Permisions.Grantees._

private[models] object Http {

  import aws.s3.signature.S3Sign

  def ressource(bucketname: Option[String], uri: String, subresource: Option[String] = None) =
    "/%s\n%s\n?%s".format(bucketname.getOrElse(""), uri, subresource.getOrElse(""))

  def request(method: Method, bucketname: Option[String] = None, subresource: Option[String] = None, body: Option[String] = None, parameters: Seq[(String, String)] = Nil): Future[Response] = {

    val uri = bucketname.map("https://" + _ + ".s3.amazonaws.com").getOrElse("https://s3.amazonaws.com") + subresource.map("/?" + _).getOrElse("")
    val res = ressource(bucketname, uri)
    // TODO: do not hardcode contentType

    val r = WS.url(uri)
      .withHeaders(
        parameters ++
        S3Sign.sign(method.toString,
          bucketname,
          subresource,
          contentType = body.map(_ => "text/plain; charset=utf-8"),
          headers = parameters,
          md5 = parameters.flatMap{
            case ("Content-MD5", v) => Seq(v) // XXX
            case _ => Nil
          }.headOption
      ): _*)

    method match {
      case PUT => r.put(body.get)
      case DELETE => r.delete()
      case GET => r.get()
      case _ => throw new RuntimeException("Unsuported method: " + method)
    }
  }

  def tryParse[T](resp: Response)(implicit p: Parser[SimpleResult[T]]) = {
    println(resp.body)
    Parser.parse[SimpleResult[T]](resp).fold( e => throw new RuntimeException(e), identity)
  }

}