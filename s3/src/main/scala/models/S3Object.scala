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

case class S3Object(name: String, value: String)
object S3Object {
  import Http._

  def put(bucketname: String) =
    request[Unit](PUT, Some(bucketname))

}