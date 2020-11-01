/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
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

package com.github.dwhjames.awswrap
package s3

import java.io.{InputStream, File}
import java.net.URL

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.Try

import java.util.concurrent.{Executors, ExecutorService, ThreadFactory}
import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.event.{ProgressListener, ProgressEvent, ProgressEventType}
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.Transfer

import org.slf4j.{Logger, LoggerFactory}


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
    this(awsCredentialsProvider, clientConfiguration, Executors.newFixedThreadPool(clientConfiguration.getMaxConnections, new S3ThreadFactory()))
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
  def shutdown(): Unit = {
    client.shutdown()
    executorService.shutdownNow()
    ()
  }


  @inline
  private def wrapMethod[Request, Result](
    f:       Request => Result,
    request: Request
  ): Future[Result] = {
    val p = Promise[Result]
    executorService.execute(new Runnable {
      override def run(): Unit = {
        p complete {
          Try {
            f(request)
          }
        }
        ()
      }
    })
    p.future
  }

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#completeMultipartUpload(com.amazonaws.services.s3.model.CompleteMultipartUploadRequest) AWS Java SDK]]
   */
  def completeMultipartUpload(
    completeMultipartUploadRequest: CompleteMultipartUploadRequest
  ): Future[CompleteMultipartUploadResult] =
    wrapMethod(client.completeMultipartUpload, completeMultipartUploadRequest)

  def abortMultipartUpload(
   abortMultipartUploadRequest: AbortMultipartUploadRequest
 ): Future[Unit] =
    wrapMethod(client.abortMultipartUpload, abortMultipartUploadRequest)

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
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#copyPart(com.amazonaws.services.s3.model.CopyPartRequest) AWS Java SDK]]
   */
  def copyPart(
    copyPartRequest: CopyPartRequest
  ): Future[CopyPartResult] =
    wrapMethod(client.copyPart, copyPartRequest)

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
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#generatePresignedUrl(com.amazonaws.services.s3.model.GeneratePresignedUrlRequest) AWS Java SDK]]
   */
  def generatePresignedUrlRequest(
    generatePresignedUrlRequest: GeneratePresignedUrlRequest
  ): Future[URL] =
    wrapMethod(client.generatePresignedUrl, generatePresignedUrlRequest)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest) AWS Java SDK]]
   */
  def getObject(
    getObjectRequest: GetObjectRequest
  ): Future[S3Object] =
    wrapMethod[GetObjectRequest, S3Object](client.getObject, getObjectRequest)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest) AWS Java SDK]]
   */
  def getObject(
    bucketName: String,
    key: String
  ): Future[S3Object] =
    getObject(new GetObjectRequest(bucketName, key))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest) AWS Java SDK]]
   */
  def getObject(
    getObjectRequest: GetObjectRequest,
    destinationFile: File
  ): Future[ObjectMetadata] =
    wrapMethod[(GetObjectRequest, File), ObjectMetadata]({ case (r: GetObjectRequest, f: File) => client.getObject(r, f) }, (getObjectRequest, destinationFile))

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
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#initiateMultipartUpload(com.amazonaws.services.s3.model.InitiateMultipartUploadRequest) AWS Java SDK]]
   */
  def initiateMultipartUpload(
    initiateMultipartUploadRequest: InitiateMultipartUploadRequest
  ): Future[InitiateMultipartUploadResult] =
    wrapMethod(client.initiateMultipartUpload, initiateMultipartUploadRequest)

  def uploadPart(
    uploadPartRequest: UploadPartRequest
  ): Future[UploadPartResult] =
    wrapMethod(client.uploadPart, uploadPartRequest)

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

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#putObject(com.amazonaws.services.s3.model.PutObjectRequest) AWS Java SDK]]
   */
  def putObject(
    putObjectRequest: PutObjectRequest
  ): Future[PutObjectResult] =
    wrapMethod[PutObjectRequest, PutObjectResult](client.putObject, putObjectRequest)

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#putObject(com.amazonaws.services.s3.model.PutObjectRequest) AWS Java SDK]]
   */
  def putObject(
    bucketName: String,
    key: String,
    file: File
  ): Future[PutObjectResult] =
    putObject(new PutObjectRequest(bucketName, key, file))

  /**
   * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#putObject(com.amazonaws.services.s3.model.PutObjectRequest) AWS Java SDK]]
   */
  def putObject(
    bucketName: String,
    key: String,
    input: InputStream,
    metadata: ObjectMetadata
  ): Future[PutObjectResult] =
    putObject(new PutObjectRequest(bucketName, key, input, metadata))
}

/**
  * A helper object providing a Scala Future interface for S3 Transfers.
  *
  * Transfers to and from S3 using the TransferManager provider a listener
  * interface, and [[FutureTransfer.listenFor]] adapts this interface to
  * Scala futures.
  *
  * @see [[com.amazonaws.services.s3.transfer.TransferManager TransferManager]]
  * @see [[com.amazonaws.services.s3.transfer.Transfer Transfer]]
  */
object FutureTransfer {

  private val logger: Logger = LoggerFactory.getLogger("com.github.dwhjames.awswrap.s3.FutureTransfer")

  /**
    * Attach a listener to an S3 Transfer and return it as a Future.
    *
    * This helper method attaches a progress and state change listeners to the given
    * Transfer object. The returned future is completed with the
    * same transfer when the transfer is ‘done’ (canceled, completed,
    * or failed). The future will always been completed successfully
    * even if the transfer itself has failed. It is up to the caller
    * to extract the result of the transfer and perform any error
    * handling.
    *
    * In essence, this helper just gives back the transfer when it is done.
    *
    * The detailed progress of the transfer is logged at debug level to the
    * `com.github.dwhjames.awswrap.s3.FutureTransfer` logger.
    *
    * @tparam T
    *     a subtype of Transfer.
    * @param transfer
    *     an S3 Transfer to listen for progress.
    * @return the transfer in a future.
    * @see [[com.amazonaws.services.s3.transfer.Transfer Transfer]]
    * @see [[com.amazonaws.event.ProgressListener ProgressListener]]
    */
  def listenFor[T <: Transfer](transfer: T): Future[transfer.type] = {
    import com.amazonaws.services.s3.transfer.internal.{ AbstractTransfer, TransferStateChangeListener }
    val transferDescription = transfer.getDescription
    def debugLog(eventType: String): Unit = {
      logger.debug(s"$eventType : $transferDescription")
    }
    def logTransferState(state: Transfer.TransferState): Unit = {
      if (logger.isDebugEnabled) {
        state match {
          case Transfer.TransferState.Waiting =>
            debugLog("Waiting")
          case Transfer.TransferState.InProgress =>
            debugLog("InProgress")
          case Transfer.TransferState.Completed =>
            debugLog("Completed")
          case Transfer.TransferState.Canceled =>
            debugLog("Canceled")
          case Transfer.TransferState.Failed =>
            debugLog("Failed")
          case _ =>
            logger.warn(s"unrecognized transfer state for transfer $transferDescription")
        }
      }
    }
    def logProgressEvent(progressEvent: ProgressEvent): Unit = {
      if (logger.isDebugEnabled) {
        progressEvent.getEventType match {
          case ProgressEventType.CLIENT_REQUEST_FAILED_EVENT =>
            debugLog("CLIENT_REQUEST_FAILED_EVENT")
          case ProgressEventType.CLIENT_REQUEST_RETRY_EVENT =>
            debugLog("CLIENT_REQUEST_RETRY_EVENT")
          case ProgressEventType.CLIENT_REQUEST_STARTED_EVENT =>
            debugLog("CLIENT_REQUEST_STARTED_EVENT")
          case ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT =>
            debugLog("CLIENT_REQUEST_SUCCESS_EVENT")
          case ProgressEventType.HTTP_REQUEST_COMPLETED_EVENT =>
            debugLog("HTTP_REQUEST_COMPLETED_EVENT")
          case ProgressEventType.HTTP_REQUEST_CONTENT_RESET_EVENT =>
            debugLog("HTTP_REQUEST_CONTENT_RESET_EVENT")
          case ProgressEventType.HTTP_REQUEST_STARTED_EVENT =>
            debugLog("HTTP_REQUEST_STARTED_EVENT")
          case ProgressEventType.HTTP_RESPONSE_COMPLETED_EVENT =>
            debugLog("HTTP_RESPONSE_COMPLETED_EVENT")
          case ProgressEventType.HTTP_RESPONSE_CONTENT_RESET_EVENT =>
            debugLog("HTTP_RESPONSE_CONTENT_RESET_EVENT")
          case ProgressEventType.HTTP_RESPONSE_STARTED_EVENT =>
            debugLog("HTTP_RESPONSE_STARTED_EVENT")
          case ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT =>
            debugLog("REQUEST_BYTE_TRANSFER_EVENT")
          case ProgressEventType.REQUEST_CONTENT_LENGTH_EVENT =>
            debugLog("REQUEST_CONTENT_LENGTH_EVENT")
          case ProgressEventType.RESPONSE_BYTE_DISCARD_EVENT =>
            debugLog("RESPONSE_BYTE_DISCARD_EVENT")
          case ProgressEventType.RESPONSE_BYTE_TRANSFER_EVENT =>
            debugLog("RESPONSE_BYTE_TRANSFER_EVENT")
          case ProgressEventType.RESPONSE_CONTENT_LENGTH_EVENT =>
            debugLog("RESPONSE_CONTENT_LENGTH_EVENT")
          case ProgressEventType.TRANSFER_CANCELED_EVENT =>
            debugLog("TRANSFER_CANCELED_EVENT")
          case ProgressEventType.TRANSFER_COMPLETED_EVENT =>
            debugLog("TRANSFER_COMPLETED_EVENT")
          case ProgressEventType.TRANSFER_FAILED_EVENT =>
            debugLog("TRANSFER_FAILED_EVENT")
          case ProgressEventType.TRANSFER_PART_COMPLETED_EVENT =>
            debugLog("TRANSFER_PART_COMPLETED_EVENT")
          case ProgressEventType.TRANSFER_PART_FAILED_EVENT =>
            debugLog("TRANSFER_PART_FAILED_EVENT")
          case ProgressEventType.TRANSFER_PART_STARTED_EVENT =>
            debugLog("TRANSFER_PART_STARTED_EVENT")
          case ProgressEventType.TRANSFER_PREPARING_EVENT =>
            debugLog("TRANSFER_PREPARING_EVENT")
          case ProgressEventType.TRANSFER_STARTED_EVENT =>
            debugLog("TRANSFER_STARTED_EVENT")
          case _ =>
            logger.warn(s"unrecognized progress event type for transfer $transferDescription")
        }
      }
    }

    val p = Promise[transfer.type]

    if (transfer.isInstanceOf[AbstractTransfer]) {
      /* Attach a state change listener to the transfer.
       * At this point, the transfer is already in progress
       * and may even have already completed. We will have
       * missed any state change events that have already been
       * fired, including the completion event!
       */
      transfer.asInstanceOf[AbstractTransfer].addStateChangeListener(new TransferStateChangeListener {
        /* Note that the transferStateChanged will be called in the Java SDK’s
         * special thread for callbacks, so any blocking calls here have
         * the potential to induce deadlock.
         */
        override def transferStateChanged(t: Transfer, state: Transfer.TransferState): Unit = {
          logTransferState(state)

          if (state == Transfer.TransferState.Completed ||
              state == Transfer.TransferState.Canceled ||
              state == Transfer.TransferState.Failed) {
            val success = p trySuccess transfer
            if (logger.isDebugEnabled) {
              if (success) {
                logger.debug(s"promise successfully completed from transfer state change listener for $transferDescription")
              }
            }
          }
        }
      })
    }

    /* Attach a progress listener to the transfer.
     * At this point, the transfer is already in progress
     * and may even have already completed. We will have
     * missed any progress events that have already been
     * fired, including the completion event!
     */
    transfer.addProgressListener(new ProgressListener {
      /* Note that the progressChanged will be called in the Java SDK’s
       * `java-sdk-progress-listener-callback-thread` special thread
       * for progress event callbacks, so any blocking calls here have
       * the potential to induce deadlock.
       */
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        logProgressEvent(progressEvent)

        val code = progressEvent.getEventType()
        if (code == ProgressEventType.TRANSFER_CANCELED_EVENT ||
            code == ProgressEventType.TRANSFER_COMPLETED_EVENT ||
            code == ProgressEventType.TRANSFER_FAILED_EVENT) {
          val success = p trySuccess transfer
          if (logger.isDebugEnabled) {
            if (success) {
              logger.debug(s"promise successfully completed from progress listener for $transferDescription")
            }
          }
        }
      }
    })

    /* In case the progress listener never fires due to the
     * transfer already being done, poll the transfer once.
     */
    if (transfer.isDone) {
      val success = p trySuccess transfer
      if (logger.isDebugEnabled) {
        if (success) {
          logger.debug(s"promise successfully completed from outside of callbacks for $transferDescription")
        }
      }
    }

    p.future
  }
}
