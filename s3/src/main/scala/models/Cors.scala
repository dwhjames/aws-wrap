package aws.s3.models

import java.util.Date

import scala.concurrent.Future
import scala.xml._

import aws.core._
import aws.core.Types._

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

import aws.s3.S3.HTTPMethods._

/**
 * Cross-Origin Resource Sharing rule
 * @see http://docs.amazonwebservices.com/AmazonS3/latest/dev/cors.html
 */
case class CORSRule(origins: Seq[String] = Nil,
                    methods: Seq[Method] = Nil,
                    headers: Seq[String] = Nil,
                    maxAge: Option[Long] = None,
                    exposeHeaders: Seq[String] = Nil)

object CORSRule {
  import Http._
  import Parameters._
  import Permisions._
  import ACLs._

  /**
   * Sets the Cross-Origin Resource Sharing configuration for your bucket.
   * If the configuration exists, Amazon S3 replaces it.
   * To use this operation, you must be allowed to perform the s3:PutCORSConfiguration action. By default, the bucket owner has this permission and can grant it to others.
   * You can add up to 100 rules to the configuration
   * @param bucketname The name of the bucket you want to set cors on.
   * @param rules List of CORSRule to apply on this Bucket
   */
  def create(bucketName: String, rules: CORSRule*) = {
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

    val ps = Seq(Parameters.MD5(b.mkString),
      Parameters.ContentLength(b.mkString.length))

    put[Node, Unit](Some(bucketName),
      body = b,
      subresource = Some("cors"),
      parameters = ps)
  }

  /**
   * Returns the cors configuration information set for the bucket.
   * To use this operation, you must have permission to perform the s3:GetCORSConfiguration action. By default, the bucket owner has this permission and can grant it to others.
   * @param bucketname The name of the bucket.
   */
  def get(bucketName: String) =
    Http.get[Seq[CORSRule]](Some(bucketName), subresource = Some("cors"))

  /**
   * Deletes the cors configuration information set for the bucket.
   * To use this operation, you must have permission to perform the s3:PutCORSConfiguration action. The bucket owner has this permission by default and can grant this permission to others.
   * @param bucketname The name of the bucket you want to delete.
   */
  def delete(bucketName: String) =
    Http.delete[Unit](Some(bucketName), subresource = Some("cors"))
}