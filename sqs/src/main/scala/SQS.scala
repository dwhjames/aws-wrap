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

import play.api.libs.iteratee.{ Iteratee, Input, Step, Done, Error => IterateeError, Enumerator }

import aws.core.{AWSError, Result, EmptyResult, Metadata}
import aws.core.modules.{HttpRequestV2Layer, SigV2Layer, UserHomeCredentialsLayer}

case class SQSMeta(requestId: String) extends Metadata

case class Queue(url: String)


trait SQSLayer extends HttpRequestV2Layer[SQSMeta] with SigV2Layer with UserHomeCredentialsLayer {

  override val apiVersionDateString = "2012-11-05"

  override protected implicit lazy val v2RequestExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  object SQS  {

    import SQSParsers._

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
          "DeleteMessageBatchRequestEntry.%d.ReceiptHandle".format(i + 1) -> message.receiptHandle)
      }).flatten
      def BatchMessageVisibility(messages: Seq[MessageVisibility]) = (for ((message, i) <- messages.zipWithIndex) yield {
        Seq(
          "ChangeMessageVisibilityBatchRequestEntry.%d.Id".format(i + 1) -> message.id,
          "ChangeMessageVisibilityBatchRequestEntry.%d.ReceiptHandle".format(i + 1) -> message.receiptHandle) ++ message.visibilityTimeout.toSeq.map(
            "ChangeMessageVisibilityBatchRequestEntry.%d.VisibilityTimeout".format(i + 1) -> _.toString)
      }).flatten
      def BatchSendEntry(messages: Seq[MessageSend]) = (for ((message, i) <- messages.zipWithIndex) yield {
        Seq(
          "SendMessageBatchRequestEntry.%d.Id".format(i + 1) -> message.id,
          "SendMessageBatchRequestEntry.%d.MessageBody".format(i + 1) -> message.body) ++ message.delaySeconds.toSeq.map(
            "SendMessageBatchRequestEntry.%d.DelaySeconds".format(i + 1) -> _.toString)
      }).flatten
      def DelaySeconds(delay: Option[Long]) = delay.toSeq.map("DelaySeconds" -> _.toString)
      def MaxNumberOfMessages(n: Option[Long]) = n.toSeq.map("MaxNumberOfMessages" -> _.toString)
      def MessageBody(message: String) = ("MessageBody" -> message)
      def MessageAttributesP(messages: Seq[MessageAttribute]) = messages.size match {
        case 0 => Nil
        case 1 => Seq("AttributeName" -> messages(0).toString)
        case _ => (for ((message, i) <- messages.zipWithIndex) yield {
          Seq(
            "AttributeName.%d".format(i + 1) -> message.toString)
        }).flatten
      }
      def QueueAttributes(attrs: Seq[QueueAttributeValue]): Seq[(String, String)] = (for ((attribute, i) <- attrs.zipWithIndex) yield {
        Seq(
          "Attribute.%d.Name".format(i + 1) -> attribute.attribute.toString,
          "Attribute.%d.Value".format(i + 1) -> attribute.value)
      }).flatten
      def QueueAttributeNames(names: Seq[QueueAttribute]): Seq[(String, String)] = names.size match {
        case 0 => Nil
        case 1 => Seq("Attribute" -> names(0).toString)
        case _ => (for ((attribute, i) <- names.zipWithIndex) yield {
          "Attribute.%d".format(i + 1) -> attribute.toString
        })
      }
      def QueueName(name: String) = ("QueueName" -> name)
      def QueueNamePrefix(prefix: String) = Option(prefix).filterNot(_ == "").toSeq.map("QueueNamePrefix" -> _)
      def QueueOwnerAWSAccountId(accountId: Option[String]) = accountId.toSeq.map("QueueOwnerAWSAccountId" -> _)
      def VisibilityTimeout(n: Option[Long]) = n.toSeq.map("VisibilityTimeout" -> _.toString)
    }

    import aws.core.Parameters.Action
    import Parameters._

    def createQueue(name: String, attributes: CreateAttributeValue*)(implicit region: SQSRegion): Future[Result[SQSMeta, Queue]] = {
      val params = Seq(Action("CreateQueue"), QueueName(name)) ++ QueueAttributes(attributes)
      Http.get[Queue](params: _*)
    }

    def listQueues(queueNamePrefix: String = "")(implicit region: SQSRegion): Future[Result[SQSMeta, Seq[Queue]]] = {
      val params = Seq(Action("ListQueues")) ++ QueueNamePrefix(queueNamePrefix)
      Http.get[Seq[Queue]](params: _*)
    }

    def deleteQueue(queueURL: String): Future[EmptyResult[SQSMeta]] = {
      val params = Seq(Action("DeleteQueue"))
      Http.get[Unit](queueURL, params: _*)
    }

    def getQueue(name: String, queueOwnerAWSAccountId: Option[String] = None)(implicit region: SQSRegion): Future[Result[SQSMeta, Queue]] = {
      val params = Seq(Action("GetQueueUrl"), QueueName(name)) ++ QueueOwnerAWSAccountId(queueOwnerAWSAccountId)
      Http.get[Queue](params: _*)
    }
  
    def sendMessage(queue: Queue, message: String, delaySeconds: Option[Long] = None): Future[Result[SQSMeta, SendMessageResult]] = {
      val params = Seq(Action("SendMessage"), MessageBody(message)) ++ DelaySeconds(delaySeconds)
      Http.get[SendMessageResult](queue.url, params: _*)
    }

    def receiveMessage(queue: Queue, attributes: Seq[MessageAttribute] = Seq(MessageAttribute.All),
                       maxNumber: Option[Long] = None,
                       visibilityTimeout: Option[Long] = None,
                       waitTimeSeconds: Option[Long] = None): Future[Result[SQSMeta, Seq[MessageReceive]]] = {
      val params = Seq(Action("ReceiveMessage")) ++
        MessageAttributesP(attributes) ++
        MaxNumberOfMessages(maxNumber) ++
        VisibilityTimeout(visibilityTimeout) ++
        waitTimeSeconds.toSeq.map("WaitTimeSeconds" -> _.toString)
      Http.get[Seq[MessageReceive]](queue.url, params: _*)
    }

    def messageEnumerator(queue: Queue, attributes: Seq[MessageAttribute] = Seq(MessageAttribute.All),
                          visibilityTimeout: Option[Long] = None): Enumerator[MessageReceive] = generateM {
      receiveMessage(queue, attributes, Some(1), visibilityTimeout, Some(20)).map {
        _ match {
          case AWSError(_, code, message) => None
          case Result(_, msgs) => msgs.headOption
        }
      }
    }

    def generateM[E](e: => Future[Option[E]]): Enumerator[E] = Enumerator.checkContinue0(new Enumerator.TreatCont0[E] {
      def apply[A](loop: Iteratee[E, A] => Future[Iteratee[E, A]], k: Input[E] => Iteratee[E, A]) = e.flatMap {
        case Some(e) => loop(k(Input.El(e)))
        case None => loop(k(Input.Empty))
      }
    })

    def getAttributes(queue: Queue, attributes: QueueAttribute*): Future[Result[SQSMeta, Seq[QueueAttributeValue]]] = {
      val params = Seq(Action("GetQueueAttributes")) ++
        QueueAttributeNames(attributes)
      Http.get[Seq[QueueAttributeValue]](queue.url, params: _*)
    }

    def setAttributes(queue: Queue, attributes: Seq[QueueAttributeValue]): Future[EmptyResult[SQSMeta]] = {
      val params = Seq(Action("SetQueueAttributes")) ++
        QueueAttributes(attributes)
      Http.get[Unit](queue.url, params: _*)
    }

    def addPermission(queue: Queue, label: String, accountIds: Seq[String], actionNames: Seq[ActionName]): Future[EmptyResult[SQSMeta]] = {
      val params = Seq(Action("AddPermission"), "Label" -> label) ++
        AccountIds(accountIds) ++
        AWSActionNames(actionNames)
      Http.get[Unit](queue.url, params: _*)
    }

    def removePermission(queue: Queue, label: String): Future[EmptyResult[SQSMeta]] = {
      Http.get[Unit](queue.url, Action("RemovePermission"), "Label" -> label)
    }

    def deleteMessage(queue: Queue, receiptHandle: String): Future[EmptyResult[SQSMeta]] = {
      Http.get[Unit](queue.url, Action("DeleteMessage"), "ReceiptHandle" -> receiptHandle)
    }

    def sendMessageBatch(queue: Queue, messages: MessageSend*): Future[Result[SQSMeta, Seq[MessageResponse]]] = {
      val params = Seq(Action("SendMessageBatch")) ++ BatchSendEntry(messages)
      Http.get[Seq[MessageResponse]](queue.url, params: _*)
    }

    def deleteMessageBatch(queue: Queue, messages: MessageDelete*): Future[Result[SQSMeta, Seq[String]]] = {
      val params = Seq(Action("DeleteMessageBatch")) ++ BatchDeleteEntry(messages)
      Http.get[Seq[String]](queue.url, params: _*)
    }

    def changeMessageVisibility(queue: Queue, receiptHandle: String, visibilityTimeout: Long): Future[EmptyResult[SQSMeta]] = {
      Http.get[Unit](queue.url,
        Action("ChangeMessageVisibility"),
        "ReceiptHandle" -> receiptHandle,
        "VisibilityTimeout" -> visibilityTimeout.toString)
    }

    def changeMessageVisibilityBatch(queue: Queue, messages: MessageVisibility*): Future[Result[SQSMeta, Seq[String]]] = {
      val params = Seq(Action("ChangeMessageVisibilityBatch")) ++ BatchMessageVisibility(messages)
      Http.get[Seq[String]](queue.url, params: _*)
    }

  }


}