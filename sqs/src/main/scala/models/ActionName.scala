package aws.sqs

trait ActionName

object ActionName {
  case object SendMessage extends ActionName
  case object ReceiveMessage extends ActionName
  case object DeleteMessage extends ActionName
  case object ChangeMessageVisibility extends ActionName
  case object GetQueueAttributes extends ActionName
  case object GetQueueUrl extends ActionName
  def apply(name: String): ActionName = name match {
    case "SendMessage" => SendMessage 
    case "ReceiveMessage" => ReceiveMessage 
    case "DeleteMessage" => DeleteMessage 
    case "ChangeMessageVisibility" => ChangeMessageVisibility 
    case "GetQueueAttributes" => GetQueueAttributes 
    case "GetQueueUrl" => GetQueueUrl 
    case _ => sys.error("Unkown action: " + name)
  }
}
