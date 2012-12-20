package com.pellucid.aws.s3.models;

import java.io.File;
import java.util.Date;
import java.util.Map;

import play.libs.Scala;
import scala.concurrent.Future;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.s3.S3;
import com.pellucid.aws.s3.S3Metadata;
import com.pellucid.aws.utils.Maps;

public class Bucket {

    private String bucketName;

    private Date creationDate;

    public static enum VersionState {
        ENABLED, SUSPENDED
    }

    public Date creationDate() {
        return this.creationDate;
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
    public Future<Result<S3Metadata, Versions>> getVersions() {
        return AWSJavaConversions.toJavaResultFuture(
                aws.s3.models.S3Object$.MODULE$.getVersions(bucketName),
                new S3.MetadataConvert(), new Mapper<aws.s3.models.Versions, Versions>() {
                    @Override public Versions apply(aws.s3.models.Versions scalaObject) {
                        return Versions.fromScala(scalaObject);
                    }
                });
    }

    /**
     * Adds an object to a bucket. You must have WRITE permissions on a bucket to add an object to it.
     * @param bucketname Name of the target bucket
     * @param body The File to upload to this Bucket
     */
    // http://aws.amazon.com/articles/1109?_encoding=UTF8&jiveRedirect=1
    // Transfer-Encoding: chunked is not supported. The PUT operation must include a Content-Length header.
    public Future<Result<S3Metadata, Object>> put(File body) {
        return S3.convertEmptyResult(aws.s3.models.S3Object$.MODULE$.put(bucketName, body));
    }

    /**
     * Removes the null version (if there is one) of an object and inserts a delete marker, which becomes the latest version of the object.
     * If there isn't a null version, Amazon S3 does not remove any objects.
     * @param bucketname Name of the target bucket
     * @param objectName Name of the object to delete
     * @param versionId specific object version to delete
     * @param mfa Required to permanently delete a versioned object if versioning is configured with MFA Delete enabled.
     */
    public Future<Result<S3Metadata, Object>> delete(String objectName, String versionId, MFA mfa) {
        return S3.convertEmptyResult(aws.s3.models.S3Object$.MODULE$.delete(
                bucketName,
                objectName,
                Scala.Option(versionId),
                Scala.Option(mfa.toScala())
                ));
    }

    public Future<Result<S3Metadata, Object>> delete(String objectName, String versionId) {
        return delete(objectName, versionId, null);
    }

    public Future<Result<S3Metadata, Object>> delete(String objectName, MFA mfa) {
        return delete(objectName, null, mfa);
    }

    public Future<Result<S3Metadata, Object>> delete(String objectName) {
        return delete(objectName, null, null);
    }

    /**
     * Delete multiple objects from a bucket using a single HTTP request
     * @param bucketname Name of the target bucket
     * @param objects Seq of objectName -> objectVersion
     * @param mfa Required to permanently delete a versioned object if versioning is configured with MFA Delete enabled.
     */
    public Future<Result<S3Metadata, BatchDeletion>> batchDelete(Map<String, String> objects, MFA mfa)  {
        return AWSJavaConversions.toJavaResultFuture(
                aws.s3.models.S3Object$.MODULE$.batchDelete(
                        bucketName,
                        AWSJavaConversions.toScalaMap(Maps.mapValues(objects, new Mapper<String, scala.Option<String>>(){
                            @Override public scala.Option<String> apply(String s) {
                                return Scala.Option(s);
                            }
                        })).toSeq(),
                        Scala.Option(mfa.toScala())),
                        new S3.MetadataConvert(), new Mapper<aws.s3.models.BatchDeletion, BatchDeletion>() {
                    @Override public BatchDeletion apply(aws.s3.models.BatchDeletion scalaObject) {
                        return BatchDeletion.fromScala(scalaObject);
                    }
                }
                );
    }

    public Future<Result<S3Metadata, BatchDeletion>> batchDelete(Map<String, String> objects)  {
        return batchDelete(objects, null);
    }

    private static scala.Enumeration.Value versionStateScala(VersionState state) {
        if (state == VersionState.ENABLED) {
            return aws.s3.S3.VersionStates$.MODULE$.ENABLED();
        } else {
            return aws.s3.S3.VersionStates$.MODULE$.SUSPENDED();
        }
    }

}
