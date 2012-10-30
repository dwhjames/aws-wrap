package aws.sns

/**
 * An endpoint to receive notifications.
 */
sealed trait Endpoint {
  def value: String
  def protocol: String
}

object Endpoint {

  def apply(endpoint: String, protocol: String) = protocol match {
    case "http" => Http(endpoint)
    case "https" => Https(endpoint)
    case "email" => Email(endpoint)
    case "email-json" => EmailJson(endpoint)
    case "sms" => SMS(endpoint)
    case "sqs" => SQS(endpoint)
    case _ => throw new IllegalArgumentException("Invalid protocol " + protocol)
  }

  case class Http(url: String) extends Endpoint {
    override val value = url
    override val protocol = "http"
  }

  case class Https(url: String) extends Endpoint {
    override val value = url
    override val protocol = "https"
  }

  case class Email(email: String) extends Endpoint {
    override val value = email
    override val protocol = "email"
  }

  case class EmailJson(email: String) extends Endpoint {
    override val value = email
    override val protocol = "email-json"
  }

  case class SMS(phoneNumber: String) extends Endpoint {
    override val value = phoneNumber
    override val protocol = "sms"
  }

  case class SQS(queueArn: String) extends Endpoint {
    override val value = queueArn
    override val protocol = "sqs"
  }

}

