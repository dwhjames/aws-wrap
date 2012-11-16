package aws.sqs

/**
 * A message, as used in Queue.sendMessageBatch
 * @param id an id that you assign to the message. Should be unique within the batch.
 */
case class MessageSend(id: String, body: String, delaySeconds: Option[Long] = None)

case class MessageDelete(id: String, receiptHandle: String)

case class MessageResponse(id: String, messageId: String, md5OfBody: String)

case class MessageVisibility(id: String, receiptHandle: String, visibilityTimeout: Option[Long] = None)
