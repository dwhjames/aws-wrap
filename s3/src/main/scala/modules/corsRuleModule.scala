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

package aws.s3
package modules

import models.CORSRule

import scala.concurrent.Future
import scala.xml.Node

import aws.core.{Result, EmptyResult}

trait CORSRuleModule {

  /**
    * Sets the Cross-Origin Resource Sharing configuration for your bucket.
    * If the configuration exists, Amazon S3 replaces it.
    * To use this operation, you must be allowed to perform the
    * s3:PutCORSConfiguration action. By default, the bucket owner has this
    * permission and can grant it to others.
    * You can add up to 100 rules to the configuration
    *
    * @param bucketname The name of the bucket you want to set cors on.
    * @param rules List of CORSRule to apply on this Bucket
    */
  def create(bucketName: String, rules: CORSRule*): Future[EmptyResult[S3Metadata]]

  /**
    * Returns the cors configuration information set for the bucket.
    * To use this operation, you must have permission to perform the
    * s3:GetCORSConfiguration action. By default, the bucket owner has this
    * permission and can grant it to others.
    *
    * @param bucketname The name of the bucket.
    */
  def get(bucketName: String): Future[Result[S3Metadata, Seq[CORSRule]]]


  /**
    * Deletes the cors configuration information set for the bucket.
    * To use this operation, you must have permission to perform the s3:PutCORSConfiguration action. The bucket owner has this permission by default and can grant this permission to others.
    * @param bucketname The name of the bucket you want to delete.
    */
  def delete(bucketName: String): Future[EmptyResult[S3Metadata]]

}

trait AbstractCORSRuleLayer {
  val CORSRule: CORSRuleModule
}

trait CORSRuleLayer extends AbstractCORSRuleLayer with AbstractHttpRequestLayer {

  override object CORSRule extends CORSRuleModule {

    def create(bucketName: String, rules: CORSRule*): Future[EmptyResult[S3Metadata]] = {
      val b =
        <CORSConfiguration>
          { for (r <- rules) yield <CORSRule>
             { for (o <- r.origins) yield <AllowedOrigin>{ o }</AllowedOrigin> }
             { for (m <- r.methods) yield <AllowedMethod>{ m }</AllowedMethod> }
             { for (h <- r.headers) yield <AllowedHeader>{ h }</AllowedHeader> }
             { for (a <- r.maxAge.toSeq) yield <MaxAgeSeconds>{ a }</MaxAgeSeconds> }
             { for (e <- r.exposeHeaders) yield <ExposeHeader>{ e }</ExposeHeader> }
           </CORSRule> }
        </CORSConfiguration>

      val ps = Seq(
        Parameters.MD5(b.mkString),
        aws.core.Parameters.ContentLength(b.mkString.length)
      )

      Http.put[Node, Unit](Some(bucketName),
        body = b,
        subresource = Some("cors"),
        parameters = ps)
    }

    def get(bucketName: String): Future[Result[S3Metadata, Seq[CORSRule]]] =
      Http.get[Seq[CORSRule]](
        Some(bucketName),
        subresource = Some("cors")
      )

    def delete(bucketName: String): Future[EmptyResult[S3Metadata]] =
      Http.delete[Unit](
        Some(bucketName),
        subresource = Some("cors")
      )
  }
}
