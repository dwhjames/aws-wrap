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

package aws

import java.util.Date
import java.text.SimpleDateFormat

/**
 * The core project contains objects and classes shared by all services.
 *  - parsing
 *  - results
 *  - signatures
 *
 */
package object core {

  /**
    * A [[Result]] with no body, for calls not returning any body (example: deleting a resource)
    */
  type EmptyResult[M <: Metadata] = Result[M, Unit]

  /**
    * A [[Result]] with no metadata, for services that don't return query metadata.
    */
  type SimpleResult[T] = Result[EmptyMeta.type, T]

  /**
    * A [[Result]] with neither metadata nor body
    */
  type EmptySimpleResult = Result[EmptyMeta.type, Unit]

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
}
