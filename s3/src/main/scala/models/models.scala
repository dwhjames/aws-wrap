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

case class S3Metadata(requestId: String, id2: String, versionId: Option[String] = None, deleteMarker: Boolean = false) extends Metadata

private[models] object Http {

  import play.api.libs.iteratee._
  import aws.s3.signature.S3Sign

  def ressource(bucketname: Option[String], uri: String, subresource: Option[String] = None) =
    "/%s\n%s\n?%s".format(bucketname.getOrElse(""), uri, subresource.getOrElse(""))

  def enumString(s: String): Enumerator[Array[Byte]] =
    Enumerator.fromStream(new java.io.ByteArrayInputStream(s.getBytes))


  def upload[T](
    method: Method,
    bucketname: String,
    objectName: String,
    body: java.io.File,
    parameters: Seq[(String, String)] = Nil)(implicit p: Parser[Result[S3Metadata, T]]): Future[Result[S3Metadata, T]] = {

      val uri = s"https://$bucketname.s3.amazonaws.com/" + objectName
      val res = ressource(Some(bucketname), uri)
      val ct = new javax.activation.MimetypesFileTypeMap().getContentType(body.getName)
      val ps = parameters :+ ("Content-Type" -> ct)

      // TODO: do not hardcode contentType
      val sign = S3Sign.sign(method.toString,
        Some(bucketname),
        Some(objectName),
        None,
        contentType = Some(ct),
        headers = ps,
        md5 = parameters.flatMap {
          case ("Content-MD5", v) => Seq(v) // XXX
          case _ => Nil
        }.headOption)

      val r = WS.url(uri)
        .withHeaders(
          (ps ++ sign): _*)

      (method match {
        case PUT => r.put(body)
        case DELETE => r.delete()
        case GET => r.get()
        case _ => throw new RuntimeException("Unsuported method: " + method)
      }).map(tryParse[T])
    }

  // TODO; refactor
  // - contentType
  def request[T](
    method: Method,
    bucketname: Option[String] = None,
    objectName: Option[String] = None,
    subresource: Option[String] = None,
    body: Option[Enumerator[Array[Byte]]] = None,
    parameters: Seq[(String, String)] = Nil)(implicit p: Parser[Result[S3Metadata, T]]): Future[Result[S3Metadata, T]] = {

    val uri = Seq(
        Some(bucketname.map("https://" + _ + ".s3.amazonaws.com").getOrElse("https://s3.amazonaws.com")),
        objectName,
        subresource.map("?" + _)).flatten.mkString("/")

    val res = ressource(bucketname, uri)

    // TODO: do not hardcode contentType
    val sign = S3Sign.sign(method.toString,
      bucketname,
      objectName,
      subresource,
      // contentType = body.map(_ => "text/plain; charset=utf-8"),
      headers = parameters,
      md5 = parameters.flatMap {
        case ("Content-MD5", v) => Seq(v) // XXX
        case _ => Nil
      }.headOption)

    val r = WS.url(uri)
      .withHeaders(
        (parameters ++ sign): _*)

    (method match {
      case PUT => r.put(body.get)
      case DELETE => r.delete()
      case GET => r.get()
      case _ => throw new RuntimeException("Unsuported method: " + method)
    }).map(tryParse[T])
  }

  private def tryParse[T](resp: Response)(implicit p: Parser[Result[S3Metadata, T]]) =
    Parser.parse[Result[S3Metadata, T]](resp).fold(e => throw new RuntimeException(e), identity)

}