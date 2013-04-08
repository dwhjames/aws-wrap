
package aws.wrap

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._

trait AmazonSQSScalaClient {

  val client: AmazonSQSAsyncClient

  def addPermission(
    addPermissionRequest: AddPermissionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.addPermissionAsync, addPermissionRequest)

  def changeMessageVisibility(
    changeMessageVisibilityRequest: ChangeMessageVisibilityRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.changeMessageVisibilityAsync, changeMessageVisibilityRequest)

  def changeMessageVisibilityBatch(
    changeMessageVisibilityBatchRequest: ChangeMessageVisibilityBatchRequest
  ): Future[ChangeMessageVisibilityBatchResult] =
    wrapAsyncMethod(client.changeMessageVisibilityBatchAsync, changeMessageVisibilityBatchRequest)

  def createQueue(
    createQueueRequest: CreateQueueRequest
  ): Future[CreateQueueResult] =
    wrapAsyncMethod(client.createQueueAsync, createQueueRequest)

  def createQueue(
    queueName: String
  ): Future[CreateQueueResult] =
    createQueue(new CreateQueueRequest(queueName))

  def deleteMessage(
    deleteMessageRequest: DeleteMessageRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteMessageAsync, deleteMessageRequest)

  def deleteMessage(
    queueUrl:      String,
    receiptHandle: String
  ): Future[Unit] =
    deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle))

  def deleteMessageBatch(
    deleteMessageBatchRequest: DeleteMessageBatchRequest
  ): Future[DeleteMessageBatchResult] =
    wrapAsyncMethod(client.deleteMessageBatchAsync, deleteMessageBatchRequest)

  def deleteMessageBatch(
    queueUrl: String,
    entries:  Seq[(String, String)]
  ): Future[DeleteMessageBatchResult] =
    deleteMessageBatch(
      new DeleteMessageBatchRequest(
        queueUrl,
        entries.map{ case (id, receiptHandle) =>
          new DeleteMessageBatchRequestEntry(id, receiptHandle)
        }.asJava
      )
    )

  def deleteQueue(
    deleteQueueRequest: DeleteQueueRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteQueueAsync, deleteQueueRequest)

  def deleteQueue(
    queueUrl: String
  ): Future[Unit] =
    deleteQueue(new DeleteQueueRequest(queueUrl))

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

  def getExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(client.getExecutorService())

  def getQueueAttributes(
    getQueueAttributesRequest: GetQueueAttributesRequest
  ): Future[GetQueueAttributesResult] =
    wrapAsyncMethod(client.getQueueAttributesAsync, getQueueAttributesRequest)

  def getQueueUrl(
    getQueueUrlRequest: GetQueueUrlRequest
  ): Future[GetQueueUrlResult] =
    wrapAsyncMethod(client.getQueueUrlAsync, getQueueUrlRequest)

  def getQueueUrl(
    queueName: String
  ): Future[GetQueueUrlResult] =
    getQueueUrl(new GetQueueUrlRequest(queueName))

  def listQueues(
    listQueuesRequest: ListQueuesRequest
  ): Future[ListQueuesResult] =
    wrapAsyncMethod(client.listQueuesAsync, listQueuesRequest)

  def listQueues(
    queueNamePrefix: String = null
  ): Future[ListQueuesResult] =
    listQueues(new ListQueuesRequest(queueNamePrefix))

  def receiveMessage(
    receiveMessageRequest: ReceiveMessageRequest
  ): Future[ReceiveMessageResult] =
    wrapAsyncMethod(client.receiveMessageAsync, receiveMessageRequest)

  def receiveMessage(
    queueUrl: String
  ): Future[ReceiveMessageResult] =
    receiveMessage(new ReceiveMessageRequest(queueUrl))

  def removePermission(
    removePermissionRequest: RemovePermissionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.removePermissionAsync, removePermissionRequest)

  def sendMessage(
    sendMessageRequest: SendMessageRequest
  ): Future[SendMessageResult] =
    wrapAsyncMethod(client.sendMessageAsync, sendMessageRequest)

  def sendMessage(
    queueUrl:    String,
    messageBody: String
  ): Future[SendMessageResult] =
    sendMessage(new SendMessageRequest(queueUrl, messageBody))

  def sendMessageBatch(
    sendMessageBatchRequest: SendMessageBatchRequest
  ): Future[SendMessageBatchResult] =
    wrapAsyncMethod(client.sendMessageBatchAsync, sendMessageBatchRequest)

  def sendMessageBatch(
    queueUrl: String,
    entries:  Seq[(String, String)]
  ): Future[SendMessageBatchResult] =
    sendMessageBatch(
      new SendMessageBatchRequest(
        queueUrl,
        entries.map{ case (id, messageBody) =>
          new SendMessageBatchRequestEntry(id, messageBody)
        }.asJava
      )
    )

  def setQueueAttributes(
    setQueueAttributesRequest: SetQueueAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.setQueueAttributesAsync, setQueueAttributesRequest)

  def shutdown(): Unit =
    client.shutdown()

}

object AmazonSQSScalaClient {

  private class AmazonSQSScalaClientImpl(override val client: AmazonSQSAsyncClient) extends AmazonSQSScalaClient

  def fromAsyncClient(client: AmazonSQSAsyncClient): AmazonSQSScalaClient =
    new AmazonSQSScalaClientImpl(client)
}
