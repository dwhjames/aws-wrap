/*
 * Copyright 2012 Pellucid and Zenexity
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import aws.s3.Permissions.Grantees._

case class S3Metadata(requestId: String, id2: String, versionId: Option[String] = None, deleteMarker: Boolean = false) extends Metadata

private[models] object Http {

  import play.api.libs.iteratee._
  import aws.s3.signature.S3Sign

  def ressource(bucketname: Option[String], uri: String, subresource: Option[String] = None) =
    "/%s\n%s\n?%s".format(bucketname.getOrElse(""), uri, subresource.getOrElse(""))

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

      val sign = S3Sign.sign(method.toString,
        Some(bucketname),
        Some(objectName),
        None,
        Nil,
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

  import play.api.http._
  private object Writes {
    implicit def contentTypeOf_Nothing = ContentTypeOf[Unit](None)
    implicit def writeableOf_Nothing[Nothing] = Writeable[Unit] { content: Unit => Array[Byte]() }
  }

  def get[T](
    bucketname: Option[String] = None,
    objectName: Option[String] = None,
    subresource: Option[String] = None,
    queryString: Seq[(String, String)] = Nil,
    parameters: Seq[(String, String)] = Nil)(implicit p: Parser[Result[S3Metadata, T]]) = {
      import Writes._
      request[Nothing, T](GET, bucketname, objectName, subresource, queryString, None, parameters)
    }

  def delete[T](
    bucketname: Option[String] = None,
    objectName: Option[String] = None,
    subresource: Option[String] = None,
    queryString: Seq[(String, String)] = Nil,
    parameters: Seq[(String, String)] = Nil)(implicit p: Parser[Result[S3Metadata, T]]) = {
      import Writes._
      request[Nothing, T](DELETE, bucketname, objectName, subresource, queryString, None, parameters)
    }

  def post[B, T](
    bucketname: Option[String] = None,
    objectName: Option[String] = None,
    subresource: Option[String] = None,
    queryString: Seq[(String, String)] = Nil,
    body: B,
    parameters: Seq[(String, String)] = Nil)(implicit w: Writeable[B], contentType: ContentTypeOf[B], p: Parser[Result[S3Metadata, T]]) =
      request[B, T](POST, bucketname, objectName, subresource, queryString, Some(body), parameters)


  def put[B, T](
    bucketname: Option[String] = None,
    objectName: Option[String] = None,
    subresource: Option[String] = None,
    queryString: Seq[(String, String)] = Nil,
    body: B,
    parameters: Seq[(String, String)] = Nil)(implicit w: Writeable[B], contentType: ContentTypeOf[B], p: Parser[Result[S3Metadata, T]]) =
      request[B, T](PUT, bucketname, objectName, subresource, queryString, Some(body), parameters)


  // TODO; refactor
  // - contentType
  private def request[B, T](
    method: Method,
    bucketname: Option[String],
    objectName: Option[String],
    subresource: Option[String],
    queryString: Seq[(String, String)],
    body: Option[B],
    parameters: Seq[(String, String)])(implicit w: Writeable[B], contentType: ContentTypeOf[B], p: Parser[Result[S3Metadata, T]]): Future[Result[S3Metadata, T]] = {

    val uri = Seq(
        Some(bucketname.map("https://" + _ + ".s3.amazonaws.com").getOrElse("https://s3.amazonaws.com")),
        objectName).flatten.mkString("/")

    val fullResource = (subresource, queryString) match {
      case (None, _) => None
      case (Some(r), Nil) => Some(r)
      case (Some(e), qs) => Some(e + "?" + queryString.map {p: (String, String) => p._1 + "=" + p._2}.mkString("&"))
    }

    val sign = S3Sign.sign(method.toString,
      bucketname,
      objectName,
      subresource,
      queryString,
      contentType = contentType.mimeType,
      headers = parameters,
      md5 = parameters.flatMap {
        case ("Content-MD5", v) => Seq(v) // XXX
        case _ => Nil
      }.headOption)

    val r = WS.url(uri)
      .withQueryString((subresource.toSeq.map(_ -> "") ++ queryString): _*)
      .withHeaders((parameters ++ sign): _*)

    (method match {
      case PUT => r.put(body.get)
      case POST => r.post(body.get)
      case DELETE => r.delete()
      case GET => r.get()
      case _ => throw new RuntimeException("Unsuported method: " + method)
    }).map(tryParse[T])
  }

  private def tryParse[T](resp: Response)(implicit p: Parser[Result[S3Metadata, T]]) =
    Parser.parse[Result[S3Metadata, T]](resp).fold(e => throw new RuntimeException(e), identity)

}
