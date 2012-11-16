package aws.sqs

object MessageAttributes extends Enumeration {
  type MessageAttribute = Value
  val All = Value("All")
  val SenderId = Value("SenderId")
  val SentTimestamp = Value("SentTimestamp")
  val ApproximateReceiveCount = Value("ApproximateReceiveCount")
  val ApproximateFirstReceiveTimestamp = Value("ApproximateFirstReceiveTimestamp")
}

