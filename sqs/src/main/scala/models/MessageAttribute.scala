package aws.sqs

sealed trait MessageAttribute

object MessageAttribute {
  case object All extends MessageAttribute
  case object SenderId extends MessageAttribute
  case object SentTimestamp extends MessageAttribute
  case object ApproximateReceiveCount extends MessageAttribute
  case object ApproximateFirstReceiveTimestamp extends MessageAttribute

  def apply(value: String): MessageAttribute = value match {
    case "All" => All
    case "SenderId" => SenderId
    case "SentTimestamp" => SentTimestamp
    case "ApproximateReceiveCount" => ApproximateReceiveCount
    case "ApproximateFirstReceiveTimestamp" => ApproximateFirstReceiveTimestamp
    case _ => sys.error("Unkown MessageAttribute " + value)
  }

}

