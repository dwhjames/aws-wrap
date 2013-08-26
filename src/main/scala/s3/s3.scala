/*
 * Copyright 2013 Pellucid Analytics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pellucid.wrap
package s3

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.{AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.Transfer


private[s3] class S3ThreadFactory extends ThreadFactory {
  private val count = new AtomicLong(0L)
  private val backingThreadFactory: ThreadFactory = Executors.defaultThreadFactory()
  override def newThread(r: Runnable): Thread = {
    val thread = backingThreadFactory.newThread(r)
    thread.setName(s"aws.wrap.s3-${count.getAndIncrement()}")
    thread
  }
}

/**
  * A lightweight wrapper for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html AmazonS3Client]]
  *
  * The AWS Java SDK does not provide an asynchronous S3 client,
  * so this class follows the approach of the asynchronous clients
  * that are provided by the SDK. Namely, to make the synchronous
  * calls within an executor service. The methods in this class
  * all return Scala futures.
  *
  * @constructor make a client from a credentials provider,
  *     a config, and an executor service.
  * @param awsCredentialsProvider
  *     a provider of AWS credentials.
  * @param clientConfiguration
  *     a client configuration.
  * @param executorService
  *     an executor service for synchronous calls to the underlying AmazonS3Client.
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html AmazonS3Client]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html AWSCredentialsProvider]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/ClientConfiguration.html ClientConfiguration]]
  * @see java.util.concurrent.ExecutorService
  */
class AmazonS3ScalaClient(
    awsCredentialsProvider: AWSCredentialsProvider,
    clientConfiguration:    ClientConfiguration,

    private[s3] val executorService: ExecutorService
) {

  /**
    * The underlying S3 client.
    *
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html AmazonS3Client]]
    */
  val client = new AmazonS3Client(awsCredentialsProvider, clientConfiguration)

  /**
    * make a client from a credentials provider, a config, and a default executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) {
    this(awsCredentialsProvider, clientConfiguration,
      new ThreadPoolExecutor(
        0, clientConfiguration.getMaxConnections,
        60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable],
        new S3ThreadFactory()))
  }

  /**
    * make a client from a credentials provider, a default config, and an executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    * @param executorService
    *     an executor service for synchronous calls to the underlying AmazonS3Client.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider, executorService: ExecutorService) {
    this(awsCredentialsProvider, new ClientConfiguration(), executorService)
  }

  /**
    * make a client from a credentials provider, a default config, and a default executor service.
    *
    * @param awsCredentialsProvider
    *     a provider of AWS credentials.
    */
  def this(awsCredentialsProvider: AWSCredentialsProvider) {
    this(awsCredentialsProvider, new ClientConfiguration())
  }

  /**
    * make a client from credentials, a config, and an executor service.
    *
    * @param awsCredentials
    *     AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    * @param executorService
    *     an executor service for synchronous calls to the underlying AmazonS3Client.
    */
  def this(awsCredentials: AWSCredentials, clientConfiguration: ClientConfiguration, executorService: ExecutorService) {
    this(new StaticCredentialsProvider(awsCredentials), clientConfiguration, executorService)
  }

  /**
    * make a client from credentials, a default config, and an executor service.
    *
    * @param awsCredentials
    *     AWS credentials.
    * @param clientConfiguration
    *     a client configuration.
    * @param executorService
    *     an executor service for synchronous calls to the underlying AmazonS3Client.
    */
  def this(awsCredentials: AWSCredentials, executorService: ExecutorService) {
    this(awsCredentials, new ClientConfiguration(), executorService)
  }

  /**
    * make a client from credentials, a default config, and a default executor service.
    *
    * @param awsCredentials
    *     AWS credentials.
    */
  def this(awsCredentials: AWSCredentials) {
    this(new StaticCredentialsProvider(awsCredentials))
  }

  /**
    * make a client from a default credentials provider, a config, and a default executor service.
    *
    * @param clientConfiguration
    *     a client configuration.
    */
  def this(clientConfiguration: ClientConfiguration) {
    this(new DefaultAWSCredentialsProviderChain(), clientConfiguration)
  }

  /**
    * make a client from a default credentials provider, a default config, and a default executor service.
    */
  def this() {
    this(new DefaultAWSCredentialsProviderChain())
  }

  /**
    * Return the underlying executor service, through which all client
    * API calls are made.
    *
    * @return the underlying executor service
    */
  def getExecutorsService(): ExecutorService = executorService

  /**
    * Shutdown the executor service.
    *
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/AmazonWebServiceClient.html#shutdown() AmazonWebServiceClient.shutdown()]]
    */
  def shutdown() {
    client.shutdown()
    executorService.shutdownNow()
  }


  @inline
  private def wrapMethod[Request, Result](
    f:       Request => Result,
    request: Request
  ): Future[Result] = {
    val p = Promise[Result]
    executorService.execute(new Runnable {
      override def run() =
        p complete {
          Try {
            f(request)
          }
        }
    })
    p.future
  }

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#copyObject(com.amazonaws.services.s3.model.CopyObjectRequest) AWS Java SDK]]
    */
  def copyObject(
    copyObjectRequest: CopyObjectRequest
  ): Future[CopyObjectResult] =
    wrapMethod(client.copyObject, copyObjectRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#copyObject(com.amazonaws.services.s3.model.CopyObjectRequest) AWS Java SDK]]
    */
  def copyObject(
    sourceBucketName:      String,
    sourceKey:             String,
    destinationBucketName: String,
    destinationKey:        String
  ): Future[CopyObjectResult] =
    copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    createBucketRequest: CreateBucketRequest
  ): Future[Bucket] =
    wrapMethod[CreateBucketRequest, Bucket](client.createBucket, createBucketRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    bucketName: String
  ): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    bucketName: String,
    region:     Region
  ): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName, region))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#createBucket(com.amazonaws.services.s3.model.CreateBucketRequest) AWS Java SDK]]
    */
  def createBucket(
    bucketName: String,
    region:     String
  ): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName, region))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteBucket(com.amazonaws.services.s3.model.DeleteBucketRequest) AWS Java SDK]]
    */
  def deleteBucket(
    deleteBucketRequest: DeleteBucketRequest
  ): Future[Unit] =
    wrapMethod[DeleteBucketRequest, Unit](client.deleteBucket, deleteBucketRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteBucket(com.amazonaws.services.s3.model.DeleteBucketRequest) AWS Java SDK]]
    */
  def deleteBucket(
    bucketName: String
  ): Future[Unit] =
    deleteBucket(new DeleteBucketRequest(bucketName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObject(com.amazonaws.services.s3.model.DeleteObjectRequest) AWS Java SDK]]
    */
  def deleteObject(
    deleteObjectRequest: DeleteObjectRequest
  ): Future[Unit] =
    wrapMethod(client.deleteObject, deleteObjectRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObject(com.amazonaws.services.s3.model.DeleteObjectRequest) AWS Java SDK]]
    */
  def deleteObject(
    bucketName: String,
    key:        String
  ): Future[Unit] =
    deleteObject(new DeleteObjectRequest(bucketName, key))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteObjects(com.amazonaws.services.s3.model.DeleteObjectsRequest) AWS Java SDK]]
    */
  def deleteObjects(
    deleteObjectsRequest: DeleteObjectsRequest
  ): Future[Seq[DeleteObjectsResult.DeletedObject]] =
    wrapMethod((req: DeleteObjectsRequest) => client.deleteObjects(req).getDeletedObjects.asScala.toSeq, deleteObjectsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteVersion(com.amazonaws.services.s3.model.DeleteVersionRequest) AWS Java SDK]]
    */
  def deleteVersion(
    deleteVersionRequest: DeleteVersionRequest
  ): Future[Unit] =
    wrapMethod(client.deleteVersion, deleteVersionRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#deleteVersion(com.amazonaws.services.s3.model.DeleteVersionRequest) AWS Java SDK]]
    */
  def deleteVersion(
    bucketName: String,
    key:        String,
    versionId:  String
  ): Future[Unit] =
    deleteVersion(new DeleteVersionRequest(bucketName, key, versionId))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#doesBucketExist(java.lang.String) AWS Java SDK]]
    */
  def doesBucketExist(
    bucketName: String
  ): Future[Boolean] =
    wrapMethod(client.doesBucketExist, bucketName)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getBucketLocation(com.amazonaws.services.s3.model.GetBucketLocationRequest) AWS Java SDK]]
    */
  def getBucketLocation(
    getBucketLocationRequest: GetBucketLocationRequest
  ): Future[String] =
    wrapMethod[GetBucketLocationRequest, String](client.getBucketLocation, getBucketLocationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getBucketLocation(com.amazonaws.services.s3.model.GetBucketLocationRequest) AWS Java SDK]]
    */
  def getBucketLocation(
    bucketName: String
  ): Future[String] =
    getBucketLocation(new GetBucketLocationRequest(bucketName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObjectMetadata(com.amazonaws.services.s3.model.GetObjectMetadataRequest) AWS Java SDK]]
    */
  def getObjectMetadata(
    getObjectMetadataRequest: GetObjectMetadataRequest
  ): Future[ObjectMetadata] =
    wrapMethod(client.getObjectMetadata, getObjectMetadataRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObjectMetadata(com.amazonaws.services.s3.model.GetObjectMetadataRequest) AWS Java SDK]]
    */
  def getObjectMetadata(
    bucketName: String,
    key:        String
  ): Future[ObjectMetadata] =
    getObjectMetadata(new GetObjectMetadataRequest(bucketName, key))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listBuckets(com.amazonaws.services.s3.model.ListBucketsRequest) AWS Java SDK]]
    */
  def listBuckets(
    listBucketsRequest: ListBucketsRequest
  ): Future[Seq[Bucket]] =
    wrapMethod((req: ListBucketsRequest) => client.listBuckets(req).asScala.toSeq, listBucketsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listBuckets(com.amazonaws.services.s3.model.ListBucketsRequest) AWS Java SDK]]
    */
  def listBuckets(): Future[Seq[Bucket]] =
    listBuckets(new ListBucketsRequest())

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest) AWS Java SDK]]
    */
  def listObjects(
    listObjectsRequest: ListObjectsRequest
  ): Future[ObjectListing] =
    wrapMethod[ListObjectsRequest, ObjectListing](client.listObjects, listObjectsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest) AWS Java SDK]]
    */
  def listObjects(
    bucketName: String
  ): Future[ObjectListing] =
    listObjects(
      new ListObjectsRequest()
      .withBucketName(bucketName)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listObjects(com.amazonaws.services.s3.model.ListObjectsRequest) AWS Java SDK]]
    */
  def listObjects(
    bucketName: String,
    prefix:     String
  ): Future[ObjectListing] =
    listObjects(
      new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest) AWS Java SDK]]
    */
  def listVersions(
    listVersionsRequest: ListVersionsRequest
  ): Future[VersionListing] =
      wrapMethod(client.listVersions, listVersionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest) AWS Java SDK]]
    */
  def listVersions(
    bucketName: String,
    prefix:     String
  ): Future[VersionListing] =
    listVersions(
      new ListVersionsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#listVersions(com.amazonaws.services.s3.model.ListVersionsRequest) AWS Java SDK]]
    */
  def listVersions(
    bucketName:      String,
    prefix:          String,
    keyMarker:       String,
    versionIdMarker: String,
    delimiter:       String,
    maxKeys:         Int
  ): Future[VersionListing] =
    listVersions(new ListVersionsRequest(bucketName, prefix, keyMarker, versionIdMarker, delimiter, maxKeys))

}

/**
  * A helper object providing a Scala Future interface for S3 Transfers.
  *
  * Transfers to and from S3 using the TransferManager provider a listener
  * interface, and [[FutureTransfer.listenFor]] adapts this interface to
  * Scala futures.
  *
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManager.html TransferManager]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/Transfer.html Transfer]]
  */
object FutureTransfer {

  /**
    * Attach a listener to an S3 Transfer and return it as a Future.
    *
    * This helper method attaches a progress listener to the given
    * Transfer object. The listener returns the transfer object in
    * a future if the transfer is completed or is cancelled, and it
    * extracts the exception on failure.
    *
    * @tparam T
    *     a subtype of Transfer.
    * @param transfer
    *     an S3 Transfer to listen for progress.
    * @return the completed or cancelled transfer in a future.
    * @throws AmazonClientException in the future the transfer failed.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/Transfer.html Transfer]]
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ProgressListener.html ProgressListener]]
    */
  def listenFor[T <: Transfer](transfer: T): Future[T] = {
    val p = Promise[T]
    transfer.addProgressListener(new ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent) {
        // listen only for 'done' states: completed, canceled, or failed
        progressEvent.getEventCode() match {
          case ProgressEvent.COMPLETED_EVENT_CODE => p.success(transfer)
          case ProgressEvent.CANCELED_EVENT_CODE  => p.success(transfer)
          case ProgressEvent.FAILED_EVENT_CODE    =>
            try {
              p.failure(transfer.waitForException())
            } catch {
              case (ex: InterruptedException) =>
            }
          case _ =>
        }
      }
    })
    p.future
  }
}
