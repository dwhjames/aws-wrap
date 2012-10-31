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
  }

  import AWS.Parameters._
  import Parameters._

  def createQueue(name: String, attributes: QueueAttribute*)(implicit region: SQSRegion): Future[Result[SQSMeta, CreateQueueResult]] = {
    val params = Seq(Action("CreateQueue"), QueueName(name)) ++ QueueAttributes(attributes)
    get[CreateQueueResult](params:_*)
  }

  def listQueues(queueNamePrefix: String = "")(implicit region: SQSRegion): Future[Result[SQSMeta, QueuesList]] = {
    val params = Seq(Action("ListQueues")) ++ QueueNamePrefix(queueNamePrefix)
    get[QueuesList](params:_*)
  }

  def deleteQueue(queueURL: String)(implicit region: SQSRegion): Future[EmptyResult[SQSMeta]] = {
    val params = Seq(Action("DeleteQueue"))
    get[Unit](queueURL, params:_*)
  }

  // GetQueueAttributes

  // SetQueueAttributes

  // SendMessage

  // ReceiveMessage

  // DeleteMessage

  // AddPermission

  // RemovePermission

  // ChangeMessageVisibility

  // GetQueueUrl

  // SendMessageBatch

  // DeleteMessageBatch

  //ChangeMessageVisibilityBatch

}

