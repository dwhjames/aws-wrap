package aws.sns

sealed trait Endpoint {
  def value: String
  def protocol: String
}

// TODO: Use macros to validate the inputs?
object Endpoint {

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

