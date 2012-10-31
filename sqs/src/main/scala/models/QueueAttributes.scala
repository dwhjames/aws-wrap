package aws.sqs

import play.api.libs.json.JsValue

sealed trait QueueAttribute {
  def name: String
  def value: String
}

object QueueAttribute {

  case class VisibilityTimeout(timeout: Long) extends QueueAttribute {
    override def name = "VisibilityTimeout"
    override def value = timeout.toString
  }

  case class Policy(policy: JsValue) extends QueueAttribute {
    override def name = "Policy"
    override def value = policy.toString
  }

  case class MaximumMessageSize(size: Long) extends QueueAttribute {
    override def name = "MaximumMessageSize"
    override def value = size.toString
  }

  case class MessageRetentionPeriod(period: Long) extends QueueAttribute {
    override def name = "MessageRetentionPeriod"
    override def value = period.toString
  }

  case class DelaySeconds(seconds: Long) extends QueueAttribute {
    override def name = "DelaySeconds"
    override def value = seconds.toString
  }

}
