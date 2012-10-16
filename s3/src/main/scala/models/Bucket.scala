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

case class Bucket(name: String, creationDate: Date)
object Bucket {
  import Http._
  import Parameters._
  import Permisions._
  import ACLs._

  def create(bucketname: String, acls: Option[ACL] = None, permissions: Seq[Grant] = Nil)(implicit region: AWSRegion): Future[EmptySimpleResult] = {
    val body =
      <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>{ region.subdomain }</LocationConstraint>
      </CreateBucketConfiguration>

    val ps = acls.map(X_AMZ_ACL(_)).toSeq ++ permissions
    request(PUT, Some(bucketname), body = Some(body.toString), parameters = ps).map(tryParse[Unit])
  }

  def delete(bucketname: String): Future[EmptySimpleResult] =
    request(DELETE, Some(bucketname)).map(tryParse[Unit])

  def list(): Future[SimpleResult[Seq[Bucket]]] =
    request(GET).map(tryParse[Seq[Bucket]])
}