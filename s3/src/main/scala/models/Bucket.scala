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

case class Bucket(name: String, creationDate: Date)
object Bucket {
  import Http._
  import Parameters._
  import Permisions._
  import ACLs._

  def create(bucketname: String, acls: Option[ACL] = None, permissions: Seq[Grant] = Nil)(implicit region: AWSRegion): Future[EmptyResult[S3Metadata]] = {
    val b =
      <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>{ region.subdomain }</LocationConstraint>
      </CreateBucketConfiguration>

    val ps = acls.map(X_AMZ_ACL(_)).toSeq ++ permissions
    put[Node, Unit](Some(bucketname), body = b, parameters = ps)
  }

  def setVersioningConfiguration(bucketname: String, versionState: VersionStates.VersionState, mfaDeleteState: Option[MFADeleteStates.MFADeleteState] = None) = {

    // TODO: implement MFA support
    mfaDeleteState.map { _ => ??? }

    val b =
      <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Status>{ versionState }</Status>
        { for(m <- mfaDeleteState.toSeq) yield <MfaDelete>{ m }</MfaDelete> }
      </VersioningConfiguration>

    val ps = Seq(Parameters.ContentLength(b.mkString.length))

    put[Node, Unit](Some(bucketname),
      body = b,
      subresource = Some("versioning"),
      parameters = ps)
  }

  def delete(bucketname: String): Future[EmptyResult[S3Metadata]] =
    Http.delete[Unit](bucketname = Some(bucketname))

  def list(): Future[Result[S3Metadata, Seq[Bucket]]] =
    get[Seq[Bucket]]()
}