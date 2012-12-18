package com.pellucid.aws.s3.models;

import java.util.Date;

import play.libs.Scala;
import scala.concurrent.Future;

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

    private static scala.Enumeration.Value versionStateScala(VersionState state) {
        if (state == VersionState.ENABLED) {
            return aws.s3.S3.VersionStates$.MODULE$.ENABLED();
        } else {
            return aws.s3.S3.VersionStates$.MODULE$.SUSPENDED();
        }
    }

}
