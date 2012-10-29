package aws.sns

import play.api.libs.json._

case class Message(default: String,
                   http: Option[String] = None,
                   https: Option[String] = None,
                   email: Option[String] = None,
                   emailJson: Option[String] = None,
                   sqs: Option[String] = None) {

  def serialize: String = this match {
    case Message(d, None, None, None, None, None) => d
    case Message(d, h, hs, e, ej, s) => Json.obj(
      "default" -> d,
      "http" -> h,
      "https" -> hs,
      "email" -> e,
      "emailJson" -> ej,
      "sqs" -> s).toString
  }

  def json: Boolean = this match {
    case Message(d, None, None, None, None, None) => false
    case _ => true
  }

}

