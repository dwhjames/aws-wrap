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

  /**
   * Create a bucket
   * Anonymous requests are never allowed to create buckets. By creating the bucket, you become the bucket owner.
   * Not every string is an acceptable bucket name.
   * @see http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?UsingBucket.html
   * @param bucketname The name of the bucket you want to create.
   * @param acls predifined grants for this Bucket
   * @param permissions Explicit access permissions
   * @param region Physical location of the bucket
   */
  def create(bucketname: String, acls: Option[ACL] = None, permissions: Seq[Grant] = Nil)(implicit region: AWSRegion): Future[EmptyResult[S3Metadata]] = {
    val b =
      <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>{ region.subdomain }</LocationConstraint>
      </CreateBucketConfiguration>

    val ps = acls.map(X_AMZ_ACL(_)).toSeq ++ permissions
    put[Node, Unit](Some(bucketname), body = b, parameters = ps)
  }

  /**
   * Set the versioning state of an existing bucket
   * To set the versioning state, you must be the bucket owner.
   * @param bucketname The name of the bucket you want to set version on.
   * @param versionState Versioning state of the bucket
   * @param mfaDeleteState Specifies whether MFA Delete is enabled in the bucket versioning configuration. When enabled, the bucket owner must include the x-amz-mfa request header in requests to change the versioning state of a bucket and to permanently delete a versioned object.
   */
  def setVersioningConfiguration(bucketname: String, versionState: VersionStates.VersionState, mfaDeleteState: Option[(MFADeleteStates.MFADeleteState, MFA)] = None) = {

    val b =
      <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Status>{ versionState }</Status>
        { for (m <- mfaDeleteState.toSeq) yield <MfaDelete>{ m._1 }</MfaDelete> }
      </VersioningConfiguration>

    val ps = Seq(AWS.Parameters.ContentLength(b.mkString.length)) ++ mfaDeleteState.map(m => Parameters.X_AMZ_MFA(m._2)).toSeq

    put[Node, Unit](Some(bucketname),
      body = b,
      subresource = Some("versioning"),
      parameters = ps)
  }

  /**
   * Delete the bucket.
   * All objects (including all object versions and Delete Markers) in the bucket must be deleted before the bucket itself can be deleted.
   * @param bucketname The name of the bucket you want to delete.
   */
  def delete(bucketname: String): Future[EmptyResult[S3Metadata]] =
    Http.delete[Unit](bucketname = Some(bucketname))

  /**
   * Returns a list of all buckets owned by the authenticated sender of the request.
   * Anonymous users cannot list buckets, and you cannot list buckets that you did not create.
   */
  def list(): Future[Result[S3Metadata, Seq[Bucket]]] =
    get[Seq[Bucket]]()
}