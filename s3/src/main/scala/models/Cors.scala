package aws.s3.models

import java.util.Date

import scala.concurrent.Future
import scala.xml.Elem

import aws.core._
import aws.core.Types._

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

import aws.s3.S3.HTTPMethods._

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

  def create(bucketName: String, rules: CORSRule*) = {
    val body =
      <CORSConfiguration>
        {
          for (r <- rules) yield <CORSRule>
                                   {
                                     for (o <- r.origins)
                                       yield <AllowedOrigin>{ o }</AllowedOrigin>
                                   }
                                   {
                                     for (m <- r.methods)
                                       yield <AllowedMethod>{ m }</AllowedMethod>
                                   }
                                   {
                                     for (h <- r.headers)
                                       yield <AllowedHeader>{ h }</AllowedHeader>
                                   }
                                   {
                                     for (a <- r.maxAge.toSeq)
                                       yield <MaxAgeSeconds>{ a }</MaxAgeSeconds>
                                   }
                                   {
                                     for (e <- r.exposeHeaders)
                                       yield <ExposeHeader>{ e }</ExposeHeader>
                                   }
                                 </CORSRule>
        }
      </CORSConfiguration>

    val ps = Seq(Parameters.MD5(body.mkString))
    request[Unit](PUT, Some(bucketName), body = Some(body.mkString), subresource = Some("cors"), parameters = ps)
  }

  def get(bucketName: String) =
    request[Seq[CORSRule]](GET, Some(bucketName), subresource = Some("cors"))

  def delete(bucketName: String) =
    request[Unit](DELETE, Some(bucketName), subresource = Some("cors"))
}