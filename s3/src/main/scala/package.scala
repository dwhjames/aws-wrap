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

import core.{Metadata, Result}
import core.parsers.{Parser, Success}
import core.utils.Crypto

import java.lang.{Boolean => JBool}
import java.net.URI

import play.api.libs.json.{JsArray, JsSuccess, Reads}

/**
 * Access to Amazon [[http://aws.amazon.com/documentation/s3 S3]].
 *
 * Amazon Simple Storage Service (Amazon S3) is a web service that enables you to store data in the cloud.
 *
 */
package object s3 {

  /**
    * Override the reading of Json arrays
    *
    * This is necessary because AWS returns single element arrays, as single values
    * {"foo": ["bar"]} is serialized as {"foo": "bar"}
    */
  implicit def awsSeqReads[T](implicit r: Reads[T]) = Reads[Seq[T]] {
    case JsArray(a) => JsSuccess(a.map(_.as[T]))
    case json       => r.reads(json).map(Seq(_))
  }

  case class MFA(
    serial: String,
    token: String
  )

  case class S3Metadata(
    requestId:    String,
    id2:          String,
    versionId:    Option[String] = None,
    deleteMarker: Boolean        = false
  ) extends Metadata

  object S3Metadata {

    implicit def s3MetadataParser = Parser[S3Metadata] { r =>
      Success(
        S3Metadata(
          requestId    = r.header("x-amz-request-id").get,
          id2          = r.header("x-amz-id-2").get,
          versionId    = r.header("x-amz-version-id"),
          deleteMarker = r.header("x-amz-delete-marker").map(JBool.parseBoolean).getOrElse(false)
        )
      )
    }

    implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[S3Metadata, T]] =
      Parser.xmlErrorParser[S3Metadata].or(Parser.resultParser(s3MetadataParser, p))
  }

  object Parameters {

    def MD5(content: String) =
      (
        "Content-MD5" ->
        Crypto.base64(java.security.MessageDigest.getInstance("MD5").digest(content.getBytes))
      )

    def X_AMZ_META(name: String, value: String) =
      (s"x-amz-meta-$name" -> value)

    def X_AMZ_SERVER_SIDE_ENCRYPTION(s: String) = {
      if(s != "AES256")
        throw new RuntimeException(s"Unsupported server side encoding: $s, the only valid value is AES256")
      ("x-amz-server-side-encryption" -> s)
    }

    def X_AMZ_STORAGE_CLASS(s: StorageClass.Value) =
      ("x-amz-storage-class" -> s.toString)

    def X_AMZ_WEBSITE_REDIRECT_LOCATION(s: URI) =
      ("x-amz-website-redirect-location" -> s.toString)

    def X_AMZ_MFA(mfa: MFA) =
      ("x-amz-mfa" -> s"${mfa.serial} ${mfa.token}")

  }

}
