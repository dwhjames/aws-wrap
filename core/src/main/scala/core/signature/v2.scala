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

package aws.core.signature

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.ws._

import aws.core._
import aws.core.parsers._
import aws.core.utils._

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
    get[T]("https://" + region.host + "/", parameters:_*)

  protected def get[T](resource: String, parameters: (String, String)*)(implicit p: Parser[Result[M, T]]): Future[Result[M, T]] =
    request(resource, parameters).map(tryParse[T])

  protected def signedUrl(method: String, url: String, params: Seq[(String, String)]): String = {

    import AWS.Parameters._
    import aws.core.SignerEncoder.encode

    val ps = Seq(
      TimeStamp(new java.util.Date()),
      AWSAccessKeyId(AWS.key),
      Version(version),
      SignatureVersion(SIGVERSION),
      SignatureMethod(SIGMETHOD))

    val queryString = canonicalQueryString(params ++ ps)

    val toSign = "%s\n%s\n%s\n%s".format(method, host(url), path(url), queryString)

    "Signature=" + encode(signature(toSign)) + "&" + queryString
  }

  private def signature(data: String) = Crypto.base64(Crypto.hmacSHA1(data.getBytes(), AWS.secret))

  private def path(url: String) = "/" + url.split("/").drop(3).mkString("/")

  private def host(url: String) = url.split("/").drop(2).head
 

}

