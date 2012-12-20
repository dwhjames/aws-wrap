package com.pellucid.aws.s3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import play.libs.Scala;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;
import akka.dispatch.Mapper;

import com.pellucid.aws.internal.AWSJavaConversions;
import com.pellucid.aws.results.Result;
import com.pellucid.aws.s3.models.Bucket;
import com.pellucid.aws.utils.Lists;

public class S3 {

    public enum HTTPMethod {
        PUT, POST, DELETE, GET
    }

    private aws.s3.S3Region scalaRegion;

    public S3(S3Region region) {
        this.scalaRegion = scalaRegion(region);
    }

    public aws.s3.S3Region scalaRegion() {
        return scalaRegion;
    }

    static scala.Enumeration.Value toScalaMethod(HTTPMethod method) {
        switch (method) {
        case PUT: return aws.s3.S3.HTTPMethods$.MODULE$.PUT();
        case POST: return aws.s3.S3.HTTPMethods$.MODULE$.POST();
        case DELETE: return aws.s3.S3.HTTPMethods$.MODULE$.DELETE();
        case GET: return aws.s3.S3.HTTPMethods$.MODULE$.GET();
        }
        return null;
    }

    public static HTTPMethod fromScalaMethod(scala.Enumeration.Value scalaMethod) {
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.PUT()) return HTTPMethod.PUT;
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.POST()) return HTTPMethod.POST;
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.DELETE()) return HTTPMethod.DELETE;
        if (scalaMethod == aws.s3.S3.HTTPMethods$.MODULE$.GET()) return HTTPMethod.GET;
        return null;
    }

    static aws.s3.S3Region scalaRegion(S3Region region) {
        switch (region) {
        case US_EAST_1: return aws.s3.S3Region$.MODULE$.US_EAST_1();
        case US_WEST_1: return aws.s3.S3Region$.MODULE$.US_WEST_1();
        case US_WEST_2: return aws.s3.S3Region$.MODULE$.US_WEST_2();
        case EU_WEST_1: return aws.s3.S3Region$.MODULE$.EU_WEST_1();
        case SA_EAST_1: return aws.s3.S3Region$.MODULE$.SA_EAST_1();
        }
        return aws.s3.S3Region$.MODULE$.DEFAULT();
    }

    public static final String GRANT_READ = "x-amz-grant-read";
    public static final String GRANT_WRITE = "x-amz-grant-write";
    public static final String GRANT_READ_ACP = "x-amz-grant-read-acp";
    public static final String GRANT_WRITE_ACP = "x-amz-grant-write-acp";
    public static final String GRANT_FULL_CONTROL = "x-amz-grant-full-control";

    public static final String ACL_PRIVATE = "private";
    public static final String ACL_PUBLIC_READ = "public-read";
    public static final String ACL_PUBLIC_READ_WRITE = "public-read-write";
    public static final String ACL_AUTHENTICATED_READ = "authenticated-read";
    public static final String ACL_BUCKET_OWNER_READ = "bucket-owner_read";
    public static final String ACL_BUCKET_OWNER_FULL_CONTROL = "bucket-owner-full-control";

    /**
     * Create a bucket
     * Anonymous requests are never allowed to create buckets. By creating the bucket, you become the bucket owner.
     * Not every string is an acceptable bucket name.
     * @see http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?UsingBucket.html
     * @param bucketname The name of the bucket you want to create.
     * @param acls predefined grants for this Bucket
     * @param permissions Explicit access permissions
     * @param region Physical location of the bucket
     */
    public Future<Result<S3Metadata, Bucket>> createBucket(final String bucketname, String acls, Map<String, String> permissions) {
        return AWSJavaConversions.toJavaResultFuture(aws.s3.models.Bucket.create(bucketname, Scala.Option(acls), null, scalaRegion),
                new MetadataConvert(),
                new Mapper<BoxedUnit, Bucket>() {
            @Override public Bucket apply(BoxedUnit unit) {
                return new Bucket(bucketname);
            }
        });
    }

    public Future<Result<S3Metadata, Bucket>> createBucket(String bucketname, String acls) {
        return createBucket(bucketname, acls, new HashMap<String, String>());
    }

    public Future<Result<S3Metadata, Bucket>> createBucket(String bucketname, Map<String, String> permissions) {
        return createBucket(bucketname, null, permissions);
    }

    public Future<Result<S3Metadata, Bucket>> createBucket(String bucketname) {
        return createBucket(bucketname, null, new HashMap<String, String>());
    }

    /**
     * Delete the bucket.
     * All objects (including all object versions and Delete Markers) in the bucket must be deleted before the bucket itself can be deleted.
     * @param bucketname The name of the bucket you want to delete.
     */
    public Future<Result<S3Metadata, Object>> deleteBucket(String bucketname) {
        return convertEmptyResult(aws.s3.models.Bucket.delete(bucketname));
    }

    /**
     * Returns a list of all buckets owned by the authenticated sender of the request.
     * Anonymous users cannot list buckets, and you cannot list buckets that you did not create.
     */
    public Future<Result<S3Metadata, List<Bucket>>> listBuckets() {
        return AWSJavaConversions.toJavaResultFuture(
                aws.s3.models.Bucket.list(), new MetadataConvert(), new Mapper<Seq<aws.s3.models.Bucket>, List<Bucket>>() {
            @Override public List<Bucket> apply(Seq<aws.s3.models.Bucket> scalaBuckets) {
                return Lists.map(JavaConversions.seqAsJavaList(scalaBuckets), new Mapper<aws.s3.models.Bucket, Bucket>() {
                    @Override public Bucket apply(aws.s3.models.Bucket scalaBucket) {
                        return new Bucket(scalaBucket.name(), scalaBucket.creationDate());
                    }
                });
            }
        });
    }

    public static Future<Result<S3Metadata, Object>> convertEmptyResult(Future<aws.core.Result<aws.s3.models.S3Metadata, BoxedUnit>> scalaResult) {
        return AWSJavaConversions.toJavaResultFuture(scalaResult, new MetadataConvert(), new Mapper<BoxedUnit, Object>() {
            @Override public Object apply(BoxedUnit unit) {
                return null;
            }
        });
    }

    public static class MetadataConvert extends Mapper<aws.s3.models.S3Metadata, S3Metadata> {
        @Override
        public S3Metadata apply(aws.s3.models.S3Metadata scalaMeta) {
            return S3Metadata.fromScala(scalaMeta);
        }
    }

}
