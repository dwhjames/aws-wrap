package aws.sqs

import scala.concurrent.Future

import aws.core._
import aws.core.Types._
import aws.core.signature.V2

case class SQSMeta(requestId: String) extends Metadata

object SQS extends V2[SQSMeta](version = "2011-10-01") {

  import SQSParsers._

  private object Parameters {
    def QueueName(name: String) = ("QueueName" -> name)
    def QueueNamePrefix(prefix: String) = Option(prefix).filterNot(_ == "").toSeq.map("QueueNamePrefix" -> _)
    def QueueAttributes(attr: Seq[QueueAttribute]) = attr.map(a => (a.name -> a.value))
    def QueueOwnerAWSAccountId(accountId: Option[String]) = accountId.toSeq.map("QueueOwnerAWSAccountId" -> _)
    def Message(message: String) = ("Message" -> message)
    def DelaySeconds(delay: Option[Long]) = delay.toSeq.map("DelaySeconds" -> _.toString)
    def MaxNumberOfMessages(n: Option[Long]) = n.toSeq.map("MaxNumberOfMessages" -> _.toString)
    def VisibilityTimeout(n: Option[Long]) = n.toSeq.map("VisibilityTimeout" -> _.toString)
  }

  import AWS.Parameters._
  import Parameters._

  def createQueue(name: String, attributes: QueueAttribute*)(implicit region: SQSRegion): Future[Result[SQSMeta, String]] = {
    val params = Seq(Action("CreateQueue"), QueueName(name)) ++ QueueAttributes(attributes)
    get[String](params:_*)
  }

  def listQueues(queueNamePrefix: String = "")(implicit region: SQSRegion): Future[Result[SQSMeta, QueuesList]] = {
    val params = Seq(Action("ListQueues")) ++ QueueNamePrefix(queueNamePrefix)
    get[QueuesList](params:_*)
  }

  def deleteQueue(queueURL: String): Future[EmptyResult[SQSMeta]] = {
    val params = Seq(Action("DeleteQueue"))
    get[Unit](queueURL, params:_*)
  }

  // GetQueueAttributes

  // SetQueueAttributes

  def sendMessage(queueURL: String, message: String, delaySeconds: Option[Long] = None): Future[Result[SQSMeta, SendMessageResult]] = {
    val params = Seq(Action("SendMessage"), Message(message)) ++ DelaySeconds(delaySeconds)
    get[SendMessageResult](queueURL, params:_*)
  }

  def receiveMessage(queueURL: String, maxNumber: Option[Long] = None, visibilityTimeout: Option[Long] = None): Future[Result[SQSMeta, SendMessageResult]] = {
    val params = Seq(Action("ReceiveMessage")) ++
      MaxNumberOfMessages(maxNumber) ++
      VisibilityTimeout(visibilityTimeout)
    get[SendMessageResult](queueURL, params:_*)
  }

  // ReceiveMessage

  // DeleteMessage

  // AddPermission

  // RemovePermission

  // ChangeMessageVisibility

  def getQueueUrl(name: String, queueOwnerAWSAccountId: Option[String] = None)(implicit region: SQSRegion): Future[Result[SQSMeta, String]] = {
    val params = Seq(Action("GetQueueUrl"), QueueName(name)) ++ QueueOwnerAWSAccountId(queueOwnerAWSAccountId)
    get[String](params:_*)
  }

  // SendMessageBatch

  // DeleteMessageBatch

  //ChangeMessageVisibilityBatch

}

