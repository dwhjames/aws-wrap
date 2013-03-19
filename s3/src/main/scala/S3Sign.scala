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

package aws.s3.signature

import java.util.Date

import aws.core._
import aws.core.utils._

object S3Sign {

  val VERSION = "2009-04-15"
  val SIGVERSION = "2"
  val SIGMETHOD = "HmacSHA1"

  def canonicalizedResource(bucketName: Option[String],
                            resource: Option[String] = None,
                            subresource: Option[String] = None,
                            queryString: Seq[(String, String)] = Nil) = {
    val qsString = queryString match {
      case Nil => ""
      case qs => "?" + queryString.map {p: (String, String) => p._1 + "=" + p._2}.mkString("&")
    }

    "%s%s%s%s".format(bucketName.map("/" + _.toLowerCase).getOrElse(""),
                      resource.map("/" + _).getOrElse("/"),
                      subresource.map("?" + _).getOrElse(""),
                      qsString)
  }

  def canonicalizedAmzHeaders(headers: Seq[(String, String)]) = {
    headers.filter(_._1.startsWith("x-amz")) match {
      case Nil => ""
      case _ => headers.map {
        case (k, v) =>
          k.toLowerCase -> v
      }.sortBy(_._1).map { case (k, v) => k + ":" + v }.mkString("\n") + "\n"
    }
  }

  def toSign(method: String, md5: String, contentType: String, date: String, amzheaders: String, resources: String) =
    "%s\n%s\n%s\n%s\n%s%s".format(method, md5, contentType, date, amzheaders, resources)

  def sign(method: String,
           bucketname: Option[String],
           objectName: Option[String],
           subresource: Option[String],
           queryString: Seq[(String, String)],
           md5: Option[String] = None,
           contentType: Option[String] = None,
           headers: Seq[(String, String)] = Nil): Seq[(String, String)] = {

    import aws.s3.AWS.Parameters._
    import aws.core.SignerEncoder.encode

    val d = aws.s3.AWS.httpDateFormat(new Date)

    val s = toSign(
      method,
      md5.getOrElse(""),
      contentType.getOrElse(""),
      d,
      canonicalizedAmzHeaders(headers),
      canonicalizedResource(bucketname, objectName, subresource, queryString))

    ("Authorization" -> ("AWS " + aws.s3.AWS.awsKey + ":" + signature(s))) :: ("Date" -> d) :: Nil
  }

  private def signature(data: String) = Crypto.base64(Crypto.hmacSHA1(data.getBytes(), aws.s3.AWS.awsSecret))

}

