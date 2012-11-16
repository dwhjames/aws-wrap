/*
 * Copyright 2012 Pellucid and Zenexity
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

package aws.sqs

import scala.concurrent.Future

import aws.core._
import aws.core.Types._
import aws.core.signature.V2

case class SQSMeta(requestId: String) extends Metadata

object SQS extends V2[SQSMeta](version = "2012-11-05") {

  import SQSParsers._

  object ActionNames extends Enumeration {
    type ActionName = Value
    val SendMessage = Value("SendMessage")
    val ReceiveMessage = Value("ReceiveMessage")
    val DeleteMessage = Value("DeleteMessage ")
    val ChangeMessageVisibility = Value("ChangeMessageVisibility ")
    val GetQueueAttributes = Value("GetQueueAttributes ")
    val GetQueueUrl = Value("GetQueueUrl")
  }
  import ActionNames._

  object Parameters {
    def AccountIds(accountIds: Seq[String]): Seq[(String, String)] = (for ((accountId, i) <- accountIds.zipWithIndex) yield {
        "AWSAccountId.%d".format(i + 1) -> accountId
    }).toSeq
    def AWSActionNames(actionNames: Seq[ActionName]): Seq[(String, String)] = (for ((action, i) <- actionNames.zipWithIndex) yield {
        "ActionName.%d".format(i + 1) -> action.toString
    }).toSeq
    def BatchDeleteEntry(messages: Seq[MessageDelete]) = (for ((message, i) <- messages.zipWithIndex) yield {
      Seq(
        "DeleteMessageBatchRequestEntry.%d.Id".format(i + 1) -> message.id,
        "DeleteMessageBatchRequestEntry.%d.ReceiptHandle".format(i + 1) -> message.receiptHandle
      )
    }).flatten
    def BatchMessageVisibility(messages: Seq[MessageVisibility]) = (for ((message, i) <- messages.zipWithIndex) yield {
      Seq(
        "ChangeMessageVisibilityBatchRequestEntry.%d.Id".format(i + 1) -> message.id,
        "ChangeMessageVisibilityBatchRequestEntry.%d.ReceiptHandle".format(i + 1) -> message.receiptHandle
      ) ++ message.visibilityTimeout.toSeq.map(
        "ChangeMessageVisibilityBatchRequestEntry.%d.VisibilityTimeout".format(i + 1) -> _.toString
      )
    }).flatten
    def BatchSendEntry(messages: Seq[MessageSend]) = (for ((message, i) <- messages.zipWithIndex) yield {
      Seq(
        "SendMessageBatchRequestEntry.%d.Id".format(i + 1) -> message.id,
        "SendMessageBatchRequestEntry.%d.MessageBody".format(i + 1) -> message.body
      ) ++ message.delaySeconds.toSeq.map(
        "SendMessageBatchRequestEntry.%d.DelaySeconds".format(i + 1) -> _.toString
      )
    }).flatten
    def DelaySeconds(delay: Option[Long]) = delay.toSeq.map("DelaySeconds" -> _.toString)
    def MaxNumberOfMessages(n: Option[Long]) = n.toSeq.map("MaxNumberOfMessages" -> _.toString)
    def Message(message: String) = ("Message" -> message)
    def QueueAttributes(attrs: Seq[QueueAttribute]): Seq[(String, String)] = (for ((attribute, i) <- attrs.zipWithIndex) yield {
      Seq(
        "Attribute.%d.Name".format(i + 1) -> attribute.name,
        "Attribute.%d.Value".format(i + 1) -> attribute.value
      )
    }).flatten
    def QueueAttributeNames(names: Seq[String]): Seq[(String, String)] = (for ((attribute, i) <- names.zipWithIndex) yield {
        "Attribute.%d".format(i + 1) -> attribute
    })
    def QueueName(name: String) = ("QueueName" -> name)
    def QueueNamePrefix(prefix: String) = Option(prefix).filterNot(_ == "").toSeq.map("QueueNamePrefix" -> _)
    def QueueOwnerAWSAccountId(accountId: Option[String]) = accountId.toSeq.map("QueueOwnerAWSAccountId" -> _)
    def VisibilityTimeout(n: Option[Long]) = n.toSeq.map("VisibilityTimeout" -> _.toString)
  }

  import AWS.Parameters._
  import Parameters._

  def createQueue(name: String, attributes: QueueAttribute*)(implicit region: SQSRegion): Future[Result[SQSMeta, Queue]] = {
    val params = Seq(Action("CreateQueue"), QueueName(name)) ++ QueueAttributes(attributes)
    get[Queue](params: _*)
  }

  def listQueues(queueNamePrefix: String = "")(implicit region: SQSRegion): Future[Result[SQSMeta, Seq[Queue]]] = {
    val params = Seq(Action("ListQueues")) ++ QueueNamePrefix(queueNamePrefix)
    get[Seq[Queue]](params: _*)
  }

  def deleteQueue(queueURL: String): Future[EmptyResult[SQSMeta]] = {
    val params = Seq(Action("DeleteQueue"))
    get[Unit](queueURL, params: _*)
  }

  def getQueue(name: String, queueOwnerAWSAccountId: Option[String] = None)(implicit region: SQSRegion): Future[Result[SQSMeta, Queue]] = {
    val params = Seq(Action("GetQueueUrl"), QueueName(name)) ++ QueueOwnerAWSAccountId(queueOwnerAWSAccountId)
    get[Queue](params: _*)
  }

  case class Queue(url: String) {

    def sendMessage(message: String, delaySeconds: Option[Long] = None): Future[Result[SQSMeta, SendMessageResult]] = {
      val params = Seq(Action("SendMessage"), Message(message)) ++ DelaySeconds(delaySeconds)
      SQS.get[SendMessageResult](this.url, params: _*)
    }

    // TODO: Add support for long polling
    def receiveMessage(maxNumber: Option[Long] = None, visibilityTimeout: Option[Long] = None): Future[Result[SQSMeta, SendMessageResult]] = {
      val params = Seq(Action("ReceiveMessage")) ++
        MaxNumberOfMessages(maxNumber) ++
        VisibilityTimeout(visibilityTimeout)
      get[SendMessageResult](this.url, params: _*)
    }

    def getAttributes(attributes: Seq[String]): Future[Result[SQSMeta, Seq[QueueAttribute]]] = {
      val params = Seq(Action("GetQueueAttribute")) ++
        QueueAttributeNames(attributes)
      SQS.get[Seq[QueueAttribute]](this.url, params: _*)
    }

    def setAttributes(attributes: Seq[QueueAttribute]): Future[EmptyResult[SQSMeta]] = {
      val params = Seq(Action("SetQueueAttributes")) ++
        QueueAttributes(attributes)
      SQS.get[Unit](this.url, params: _*)
    }

    def addPermission(label: String, accountIds: Seq[String], actionNames: Seq[ActionName]): Future[EmptyResult[SQSMeta]] = {
      val params = Seq(Action("AddPermission"), "Label" -> label) ++
        AccountIds(accountIds) ++
        AWSActionNames(actionNames)
      SQS.get[Unit](this.url, params: _*)
    }

    def removePermission(label: String): Future[EmptyResult[SQSMeta]] = {
      SQS.get[Unit](this.url, Action("RemovePermission"), "Label" -> label)
    }

    def deleteMessage(receiptHandle: String): Future[EmptyResult[SQSMeta]] = {
      SQS.get[Unit](this.url, Action("DeleteMessage"), "ReceiptHandle" -> receiptHandle)
    }

    def sendMessageBatch(messages: Seq[MessageSend]): Future[Result[SQSMeta, Seq[MessageResponse]]] = {
      val params = Seq(Action("SendMessageBatch")) ++ BatchSendEntry(messages)
      SQS.get[Seq[MessageResponse]](this.url, params: _*)
    }

    def deleteMessageBatch(messages: Seq[MessageDelete]): Future[Result[SQSMeta, Seq[String]]] = {
      val params = Seq(Action("DeleteMessageBatch")) ++ BatchDeleteEntry(messages)
      SQS.get[Seq[String]](this.url, params: _*)
    }

    def changeMessageVisibility(receiptHandle: String, visibilityTimeout: Long): Future[EmptyResult[SQSMeta]] = {
      SQS.get[Unit](this.url,
        Action("ChangeMessageVisibility"),
        "ReceiptHandle" -> receiptHandle,
        "VisibilityTimeout" -> visibilityTimeout.toString
      )
    }

    def changeMessageVisibilityBatch(messages: Seq[MessageVisibility]): Future[Result[SQSMeta, Seq[String]]] = {
      val params = Seq(Action("ChangeMessageVisibilityBatch")) ++ BatchMessageVisibility(messages)
      SQS.get[Seq[String]](this.url, params: _*)
    }

  }

}

