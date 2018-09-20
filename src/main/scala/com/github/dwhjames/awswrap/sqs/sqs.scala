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
package sqs

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._

class AmazonSQSScalaClient(
    val client: AmazonSQSAsyncClient,
    implicit val execCtx: ExecutionContext
) {

  def addPermission(
    addPermissionRequest: AddPermissionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.addPermissionAsync, addPermissionRequest)

  def addPermission(
    queueUrl:       String,
    label:          String,
    accountActions: Map[String, String]
  ): Future[Unit] = {
    val (accounts, actions) = accountActions.unzip
    addPermission(
      new AddPermissionRequest(
        queueUrl,
        label,
        accounts.toSeq.asJava,
        actions.toSeq.asJava
      )
    )
  }

  def changeMessageVisibility(
    changeMessageVisibilityRequest: ChangeMessageVisibilityRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.changeMessageVisibilityAsync, changeMessageVisibilityRequest)

  def changeMessageVisibility(
    queueUrl:          String,
    receiptHandle:     String,
    visibilityTimeout: Int
  ): Future[Unit] =
    changeMessageVisibility(new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, visibilityTimeout))

  def changeMessageVisibilityBatch(
    changeMessageVisibilityBatchRequest: ChangeMessageVisibilityBatchRequest
  ): Future[ChangeMessageVisibilityBatchResult] =
    wrapAsyncMethod(client.changeMessageVisibilityBatchAsync, changeMessageVisibilityBatchRequest)

  def changeMessageVisibilityBatch(
    queueUrl:            String,
    messageVisibilities: Seq[(String, String, Int)]
  ): Future[ChangeMessageVisibilityBatchResult] =
    changeMessageVisibilityBatch(
      new ChangeMessageVisibilityBatchRequest(
        queueUrl,
        messageVisibilities.map { case (id, receiptHandle, visibilityTimeout) =>
          new ChangeMessageVisibilityBatchRequestEntry(id, receiptHandle)
          .withVisibilityTimeout(visibilityTimeout)
        } .asJava
      )
    )

  def createQueue(
    createQueueRequest: CreateQueueRequest
  ): Future[CreateQueueResult] =
    wrapAsyncMethod(client.createQueueAsync, createQueueRequest)

  def createQueue(
    queueName: String,
    attributes: Map[QueueAttributeName, Any] = Map.empty
  ): Future[CreateQueueResult] =
    createQueue(
      new CreateQueueRequest(queueName)
      .withAttributes(
        attributes.map{ case (n, v) =>
          (n.toString, v.toString)
        }.asJava
      )
    )

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

  def getQueueAttributes(
    getQueueAttributesRequest: GetQueueAttributesRequest
  ): Future[GetQueueAttributesResult] =
    wrapAsyncMethod(client.getQueueAttributesAsync, getQueueAttributesRequest)

  def getQueueAttributes(
    queueUrl: String,
    attributeNames: Seq[String]
  ): Future[Map[String, String]] =
    getQueueAttributes(
      new GetQueueAttributesRequest(queueUrl)
      .withAttributeNames(attributeNames: _*)
    ).map(_.getAttributes.asScala.toMap)

  def getQueueUrl(
    getQueueUrlRequest: GetQueueUrlRequest
  ): Future[GetQueueUrlResult] =
    wrapAsyncMethod(client.getQueueUrlAsync, getQueueUrlRequest)

  def getQueueUrl(
    queueName: String
  ): Future[String] =
    getQueueUrl(new GetQueueUrlRequest(queueName)).map(_.getQueueUrl)

  def listQueues(
    listQueuesRequest: ListQueuesRequest
  ): Future[ListQueuesResult] =
    wrapAsyncMethod(client.listQueuesAsync, listQueuesRequest)

  def listQueues(
    queueNamePrefix: String = null
  ): Future[Seq[String]] =
    listQueues(new ListQueuesRequest(queueNamePrefix)).map(_.getQueueUrls.asScala.toSeq)

  def receiveMessage(
    receiveMessageRequest: ReceiveMessageRequest
  ): Future[ReceiveMessageResult] =
    wrapAsyncMethod(client.receiveMessageAsync, receiveMessageRequest)

  def receiveMessage(
    queueUrl: String
  ): Future[Message] =
    receiveMessage(new ReceiveMessageRequest(queueUrl)).map(_.getMessages.get(0))

  def receiveMessage(
    queueUrl:            String,
    maxNumberOfMessages: Int
  ): Future[Seq[Message]] =
    receiveMessage(
      new ReceiveMessageRequest(queueUrl)
      .withMaxNumberOfMessages(maxNumberOfMessages)
    ).map(_.getMessages.asScala.toSeq)

  def removePermission(
    removePermissionRequest: RemovePermissionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.removePermissionAsync, removePermissionRequest)

  def removePermission(
    queueUrl: String,
    label:    String
  ): Future[Unit] =
    removePermission(new RemovePermissionRequest(queueUrl, label))

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

  def setQueueAttributes(
    queueUrl:   String,
    attributes: Map[String, String]
  ): Future[Unit] =
    setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributes.asJava))

  def shutdown(): Unit =
    client.shutdown()

}
