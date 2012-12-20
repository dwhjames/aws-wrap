package com.pellucid.aws.s3.models;

import java.util.Date;

import play.libs.Scala;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.s3.S3;
import com.pellucid.aws.s3.S3Metadata;


public class Bucket {

    private String bucketName;

    private Date creationDate;

    public static enum VersionState {
        ENABLED, SUSPENDED
    }

    public Bucket(String bucketName) {
        this(bucketName, null);
    }

    public Bucket(String bucketName, Date creationDate) {
        this.bucketName = bucketName;
        this.creationDate = creationDate;
    }

    /**
     * Set the versioning state of an existing bucket
     * To set the versioning state, you must be the bucket owner.
     * @param bucketname The name of the bucket you want to set version on.
     * @param versionState Versioning state of the bucket
     * @param mfaDeleteState Specifies whether MFA Delete is enabled in the bucket versioning configuration. When enabled, the bucket owner must include the x-amz-mfa request header in requests to change the versioning state of a bucket and to permanently delete a versioned object.
     */
    public Future<Result<S3Metadata, Object>> setVersioningConfiguration(
            VersionState versionState,
            MFAState mfaState) {
        return S3.convertEmptyResult(aws.s3.models.Bucket.setVersioningConfiguration(
                bucketName,
                versionStateScala(versionState), Scala.Option((mfaState == null) ? null : mfaState.toScala())));
    }

    /**
     * Returns some or all (up to 1000) of the objects in a bucket.
     * To use this implementation of the operation, you must have READ access to the bucket.
     */
     public Future<Result<S3Metadata, S3Object>> content() {
         return AWSJavaConversions.toJavaResultFuture(
                 aws.s3.models.S3Object$.MODULE$.content(bucketName),
                 new S3.MetadataConvert(), new Mapper<aws.s3.models.S3Object, S3Object>() {
             @Override public S3Object apply(aws.s3.models.S3Object scalaObject) {
                 return S3Object.fromScala(scalaObject);
             }
         });

     }

     /**
     * List metadata about all of the versions of objects in a bucket
     */
/*     def getVersions(bucketname: String) =
       Http.get[Versions](Some(bucketname), subresource = Some("versions"))
*/
     /**
     * Adds an object to a bucket. You must have WRITE permissions on a bucket to add an object to it.
     * @param bucketname Name of the target bucket
     * @param body The File to upload to this Bucket
     */
     // http://aws.amazon.com/articles/1109?_encoding=UTF8&jiveRedirect=1
     // Transfer-Encoding: chunked is not supported. The PUT operation must include a Content-Length header.
/*     def put(bucketname: String, body: File) =
       Http.upload[Unit](PUT, bucketname, body.getName, body)
*/
     /**
     * Removes the null version (if there is one) of an object and inserts a delete marker, which becomes the latest version of the object.
     * If there isn't a null version, Amazon S3 does not remove any objects.
     * @param bucketname Name of the target bucket
     * @param objectName Name of the object to delete
     * @param versionId specific object version to delete
     * @param mfa Required to permanently delete a versioned object if versioning is configured with MFA Delete enabled.
     */
/*     def delete(bucketname: String, objectName: String, versionId: Option[String] = None, mfa: Option[MFA] = None) = {
       Http.delete[Unit](Some(bucketname),
         Some(objectName),
         parameters = mfa.map{ m => Parameters.X_AMZ_MFA(m) }.toSeq,
         queryString = versionId.toSeq.map("versionId" -> _))
     }
*/
     /**
     * Delete multiple objects from a bucket using a single HTTP request
     * @param bucketname Name of the target bucket
     * @param objects Seq of objectName -> objectVersion
     * @param mfa Required to permanently delete a versioned object if versioning is configured with MFA Delete enabled.
     */
/*     def batchDelete(bucketname: String, objects: Seq[(String, Option[String])], mfa: Option[MFA] = None) = {
       val b =
         <Delete>
           <Quiet>false</Quiet>
           {
             for(o <- objects) yield
             <Object>
               <Key>{ o._1 }</Key>
               { for(v <- o._2.toSeq) yield <VersionId>{ v }</VersionId> }
             </Object>
           }
         </Delete>

       val ps = Seq(Parameters.MD5(b.mkString), AWS.Parameters.ContentLength(b.mkString.length)) ++
         mfa.map(m => Parameters.X_AMZ_MFA(m)).toSeq

       Http.post[Node, BatchDeletion](Some(bucketname),
         body = b,
         subresource = Some("delete"),
         parameters = ps)
     }*/

    
    private static scala.Enumeration.Value versionStateScala(VersionState state) {
        if (state == VersionState.ENABLED) {
            return aws.s3.S3.VersionStates$.MODULE$.ENABLED();
        } else {
            return aws.s3.S3.VersionStates$.MODULE$.SUSPENDED();
        }
    }

}
