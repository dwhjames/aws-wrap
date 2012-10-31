package aws.sqs

case class QueuesList(queues: Seq[String])

case class CreateQueueResult(queueURL: String)

