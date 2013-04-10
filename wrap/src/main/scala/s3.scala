
package aws.wrap

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import java.util.concurrent.{Executors, ExecutorService}

import com.amazonaws.{AmazonWebServiceRequest, ClientConfiguration}
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.Transfer

trait AmazonS3ScalaClient {

  val client: AmazonS3AsyncClient

  @inline
  private def wrapMethod[Request <: AmazonWebServiceRequest, Result](
    f:       Request => Result,
    request: Request
  ): Future[Result] = {
    val p = Promise[Result]
    client.executorService.execute(new Runnable {
      override def run() =
        p complete {
          Try {
            f(request)
          }
        }
    })
    p.future
  }

  def copyObject(copyObjectRequest: CopyObjectRequest): Future[CopyObjectResult] =
    wrapMethod(client.copyObject, copyObjectRequest)

  def copyObject(sourceBucketName: String, sourceKey: String, destinationBucketName: String, destinationKey: String): Future[CopyObjectResult] =
    copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey))

  def createBucket(createBucketRequest: CreateBucketRequest): Future[Bucket] =
    wrapMethod[CreateBucketRequest, Bucket](client.createBucket, createBucketRequest)

  def createBucket(bucketName: String): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName))

  def createBucket(bucketName: String, region: Region): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName, region))

  def createBucket(bucketName: String, region: String): Future[Bucket] =
    createBucket(new CreateBucketRequest(bucketName, region))

  def deleteBucket(deleteBucketRequest: DeleteBucketRequest): Future[Unit] =
    wrapMethod[DeleteBucketRequest, Unit](client.deleteBucket, deleteBucketRequest)

  def deleteBucket(bucketName: String): Future[Unit] =
    deleteBucket(new DeleteBucketRequest(bucketName))

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Future[Unit] =
    wrapMethod(client.deleteObject, deleteObjectRequest)

  def deleteObject(bucketName: String, key: String): Future[Unit] =
    deleteObject(new DeleteObjectRequest(bucketName, key))

  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Future[DeleteObjectsResult] =
    wrapMethod(client.deleteObjects, deleteObjectsRequest)

  def deleteVersion(deleteVersionRequest: DeleteVersionRequest): Future[Unit] =
    wrapMethod(client.deleteVersion, deleteVersionRequest)

  def deleteVersion(bucketName: String, key: String, versionId: String): Future[Unit] =
    deleteVersion(new DeleteVersionRequest(bucketName, key, versionId))

  def doesBucketExist(bucketName: String): Future[Boolean] = {
    val p = Promise[Boolean]
    client.executorService.execute(new Runnable {
      override def run() =
        p complete {
          Try {
            client.doesBucketExist(bucketName)
          }
        }
    })
    p.future
  }

  def getBucketLocation(getBucketLocationRequest: GetBucketLocationRequest): Future[String] =
    wrapMethod[GetBucketLocationRequest, String](client.getBucketLocation, getBucketLocationRequest)

  def getBucketLocation(bucketName: String): Future[String] =
    getBucketLocation(new GetBucketLocationRequest(bucketName))

  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): Future[ObjectMetadata] =
    wrapMethod(client.getObjectMetadata, getObjectMetadataRequest)

  def getObjectMetadata(bucketName: String, key: String): Future[ObjectMetadata] =
    getObjectMetadata(new GetObjectMetadataRequest(bucketName, key))

  def listBuckets(listBucketsRequest: ListBucketsRequest): Future[Seq[Bucket]] =
    wrapMethod((req: ListBucketsRequest) => client.listBuckets(req).asScala.toSeq, listBucketsRequest)

  def listBuckets(): Future[Seq[Bucket]] =
    listBuckets(new ListBucketsRequest())

  def listObjects(listObjectsRequest: ListObjectsRequest): Future[ObjectListing] =
    wrapMethod[ListObjectsRequest, ObjectListing](client.listObjects, listObjectsRequest)

  def listObjects(bucketName: String): Future[ObjectListing] =
    listObjects(
      new ListObjectsRequest()
      .withBucketName(bucketName)
    )

  def listObjects(bucketName: String, prefix: String): Future[ObjectListing] =
    listObjects(
      new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    )

  def listVersions(listVersionsRequest: ListVersionsRequest): Future[VersionListing] =
      wrapMethod(client.listVersions, listVersionsRequest)

}

class AmazonS3AsyncClient(
  awsCredentialsProvider: AWSCredentialsProvider,
  clientConfiguration:    ClientConfiguration,
  private[aws] val executorService: ExecutorService
) extends AmazonS3Client(awsCredentialsProvider, clientConfiguration) {

  // default executor service
  def this(awsCredentialsProvider: AWSCredentialsProvider, clientConfiguration: ClientConfiguration) {
    this(awsCredentialsProvider, clientConfiguration, Executors.newCachedThreadPool())
  }

  // default provider and executor service
  def this(clientConfiguration: ClientConfiguration) {
    this(new DefaultAWSCredentialsProviderChain(), clientConfiguration, Executors.newCachedThreadPool())
  }

  // wrap credentials into a static provider
  def this(awsCredentials: AWSCredentials, clientConfiguration: ClientConfiguration, executorService: ExecutorService) {
    this(new StaticCredentialsProvider(awsCredentials), clientConfiguration, executorService)
  }

  // wrap credentials into a static provider, and default client config
  def this(awsCredentials: AWSCredentials, executorService: ExecutorService) {
    this(awsCredentials, new ClientConfiguration(), executorService)
  }

  // wrap credentials into a static provider, and default client config and executor service
  def this(awsCredentials: AWSCredentials) {
    this(awsCredentials, Executors.newCachedThreadPool())
  }

  // default client config
  def this(awsCredentialsProvider: AWSCredentialsProvider, executorService: ExecutorService) {
    this(awsCredentialsProvider, new ClientConfiguration(), executorService)
  }

  // default client config and executor service
  def this(awsCredentialsProvider: AWSCredentialsProvider) {
    this(awsCredentialsProvider, Executors.newCachedThreadPool())
  }

  // default credentials provider, client config and executor service
  def this() {
    this(new DefaultAWSCredentialsProviderChain())
  }

  def getExecutorsService(): ExecutorService = executorService

  override def shutdown() {
    super.shutdown()
    executorService.shutdownNow()
  }
}

object FutureTransfer {

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
