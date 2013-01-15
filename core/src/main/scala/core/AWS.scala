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

package aws.core

import play.api.libs.ws._
import play.api.libs.ws.WS._

import java.util.Date
import java.text.SimpleDateFormat

import aws.core.utils._
import aws.core.parsers._

import scala.concurrent.Future

import com.ning.http.client.Realm.{ AuthScheme, RealmBuilder }

trait AWS {
  /**
   * The current AWS key, read from the first line of `~/.awssecret`
   */
  lazy val awsKey: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(0)

  /**
   * The current AWS secret, read from the second line of `~/.awssecret`
   */
  lazy val awsSecret: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(1)

  /**
   * The execution context for futures
   */
  implicit val defaultExecutionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  /**
   * Format the date in ISO: `yyyy-MM-dd'T'HH:mm:ssZ`
   */
  def isoDateFormat(date: Date) = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date)

  /**
   * Format the date in ISO basic, in GMT: `yyyyMMdd'T'HHmmss'Z`
   */
  def isoBasicFormat(date: Date) = {
    val iso = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    iso.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
    iso.format(date)
  }

  def canonicalQueryString(params: Seq[(String, String)]) =
    params.sortBy(_._1).map { p => SignerEncoder.encode(p._1) + "=" + SignerEncoder.encode(p._2) }.mkString("&")

  def httpDateFormat(date: Date) = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z").format(date)
  def httpDateparse(date: String) = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z").parse(date)

  object Parameters {
    def TimeStamp(date: Date) = "Timestamp" -> isoDateFormat(date)
    def Expires(seconds: Long) = "Expires" -> {
      val now = new Date().getTime()
      (now / 1000 + seconds).toString
    }
    def Action(a: String) = ("Action" -> a)
    def AWSAccessKeyId(key: String) = ("AWSAccessKeyId" -> key)
    def Version(v: String) = ("Version" -> v)
    def SignatureVersion(v: String) = ("SignatureVersion" -> v)
    def SignatureMethod(m: String) = ("SignatureMethod" -> m)

    type MimeType = String

    def CacheControl(s: String) = ("Cache-Control" -> s)
    def ContentDisposition(s: String) = ("Content-Disposition" -> s)
    def ContentEncoding(c: java.nio.charset.Charset) = ("Content-Encoding" -> c.name)
    def ContentLength(l: Long) = ("Content-Length" -> l.toString)
    def ContentType(s: MimeType) = ("Content-Type" -> s)
    def Expect(s: MimeType) = ("Expect" -> s)
    def Expires(d: java.util.Date) = ("Expires" -> httpDateFormat(d))
  }


  case class V2[M <: Metadata](val version: String = "2009-04-15") {

    private val SIGVERSION = "2"
    private val SIGMETHOD = "HmacSHA1"

    protected def request(resource: String, parameters: Seq[(String, String)]): Future[Response] = {
      WS.url(resource + "?" + signedUrl("GET", resource, parameters)).get()
    }

    private def tryParse[T](resp: Response)(implicit p: Parser[Result[M, T]]) = Parser.parse[Result[M, T]](resp).fold(
      e => throw new RuntimeException(e),
      identity)

    protected def get[T](parameters: (String, String)*)(implicit region: AWSRegion, p: Parser[Result[M, T]]): Future[Result[M, T]] =
      get[T]("https://" + region.host + "/", parameters: _*)

    protected def get[T](resource: String, parameters: (String, String)*)(implicit p: Parser[Result[M, T]]): Future[Result[M, T]] =
      request(resource, parameters).map(tryParse[T])

    protected def signedUrl(method: String, url: String, params: Seq[(String, String)]): String = {

      import aws.core.SignerEncoder.encode
      import Parameters._

      val ps = Seq(
        TimeStamp(new java.util.Date()),
        AWSAccessKeyId(awsKey),
        Version(version),
        SignatureVersion(SIGVERSION),
        SignatureMethod(SIGMETHOD))

      val queryString = canonicalQueryString(params ++ ps)

      val toSign = "%s\n%s\n%s\n%s".format(method, host(url), path(url), queryString)

      "Signature=" + encode(signature(toSign)) + "&" + queryString
    }

    private def signature(data: String) = Crypto.base64(Crypto.hmacSHA1(data.getBytes(), awsSecret))

    private def path(url: String) = "/" + url.split("/").drop(3).mkString("/")

    private def host(url: String) = url.split("/").drop(2).head

  }

  object V4 {

    val VERSION = "20111205"

    val ALGO = "AWS4-HMAC-SHA256"

    def derivedKey(date: Option[Date] = None)(implicit region: AWSRegion): Array[Byte] = {
      val formattedDate = new SimpleDateFormat("yyyyMMdd").format(date.getOrElse(new Date()))
      val kDate = Crypto.hmacSHA256(formattedDate.getBytes, ("AWS4" + awsSecret).getBytes())
      val kRegion = Crypto.hmacSHA256(region.subdomain.getBytes, kDate)
      val kService = Crypto.hmacSHA256(region.service.getBytes, kRegion)
      Crypto.hmacSHA256("aws4_request".getBytes, kService)
    }

    def canonicalHeaders(headers: Seq[(String, String)]) =
      headers.sortBy(_._1).map { h => h._1.toLowerCase + ":" + h._2.trim + "\n" }.mkString

    def signedHeaders(headers: Seq[(String, String)]) =
      headers.sortBy(_._1).map { h => h._1.toLowerCase }.mkString(";")

    def canonicalRequest(headers: Seq[(String, String)], queryString: Seq[(String, String)], body: String) = {
      "POST" + '\n' +
        "/" + '\n' +
        canonicalQueryString(queryString) + '\n' +
        canonicalHeaders(headers) + '\n' +
        signedHeaders(headers) + '\n' +
        Binary.toHex(hash(body))
    }

    /**
     * Hashes the string contents (assumed to be UTF-8) using the SHA-256
     * algorithm.
     *
     * @param text The string to hash.
     *
     * @return The hashed bytes from the specified string.
     */
    def hash(text: String): Array[Byte] = {
      import java.security.MessageDigest
      val md = MessageDigest.getInstance("SHA-256")
      md.update(text.getBytes("UTF-8"))
      md.digest()
    }

    def signature(requestTime: java.util.Date,
                  hdrs: Seq[(String, String)],
                  qs: Seq[(String, String)] = Nil,
                  body: String = "")(implicit region: AWSRegion) = {
      val stringToSign =
        ALGO + '\n' +
          isoBasicFormat(requestTime) + '\n' +
          credentialScope + '\n' +
          Binary.toHex(V4.hash(V4.canonicalRequest(hdrs, qs, body)))
      Crypto.hmacSHA256(stringToSign.getBytes(), V4.derivedKey())
    }

    def credentialScope(implicit region: AWSRegion): String =
      new SimpleDateFormat("yyyyMMdd").format(new Date()) + "/" + region.subdomain + "/" + region.service + "/aws4_request"

    def authorizationHeader(requestTime: java.util.Date,
                            headers: Seq[(String, String)],
                            qs: Seq[(String, String)] = Nil,
                            body: String = "")(implicit region: AWSRegion) = {
      val credentialsAuthorizationHeader = "Credential=" + awsKey + "/" + credentialScope;
      val signedHeadersAuthorizationHeader = "SignedHeaders=" + signedHeaders(headers);
      val signatureAuthorizationHeader = "Signature=" + Binary.toHex(signature(requestTime, headers, qs, body));

      ALGO + " " +
        credentialsAuthorizationHeader + ", " +
        signedHeadersAuthorizationHeader + ", " +
        signatureAuthorizationHeader
    }

  }

}
