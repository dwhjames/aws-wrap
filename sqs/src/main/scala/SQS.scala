package aws.sqs

import scala.concurrent.Future

import aws.core._
import aws.core.signature.V2

case class SQSMeta(requestId: String) extends Metadata

object SQS extends V2[SQSMeta](version = "2011-10-01") {

  import SQSParsers._

  private object Parameters {
    def QueueNamePrefix(prefix: String) = Option(prefix).filterNot(_ == "").toSeq.map("QueueNamePrefix" -> _)
  }

  import AWS.Parameters._
  import Parameters._

  // CreateQueue

  def listQueues(queueNamePrefix: String = "")(implicit region: SQSRegion): Future[Result[SQSMeta, QueuesList]] = {
    val params = Seq(Action("ListQueues")) ++ QueueNamePrefix(queueNamePrefix)
    get[QueuesList](params:_*)
  }

  // DeleteQueue

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

