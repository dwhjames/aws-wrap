/*
 * Copyright 2012 Pellucid and Zenexity
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aws.sqs

import play.api.libs.json.{ Json, JsValue }

object QueueAttributes extends Enumeration {
  type QueueAttribute = Value
  val All = Value("All")
  val ApproximateNumberOfMessages = Value("ApproximateNumberOfMessages")
  val ApproximateNumberOfMessagesDelayed = Value("ApproximateNumberOfMessagesDelayed")
  val ApproximateNumberOfMessagesNotVisible = Value("ApproximateNumberOfMessagesNotVisible")
  val CreatedTimestamp = Value("CreatedTimestamp")
  val DelaySeconds = Value("DelaySeconds")
  val LastModifiedTimestamp = Value("LastModifiedTimestamp")
  val MaximumMessageSize = Value("MaximumMessageSize")
  val MessageRetentionPeriod = Value("MessageRetentionPeriod")
  val OldestMessageAge = Value("OldestMessageAge")
  val Policy = Value("Policy")
  val QueueArn = Value("QueueArn")
  val ReceiveMessageWaitTimeSeconds = Value("ReceiveMessageWaitTimeSeconds")
  val VisibilityTimeout = Value("VisibilityTimeout")
}
import QueueAttributes._

trait QueueAttributeValue {
  def attribute: QueueAttribute
  def value: String
}

/**
 * A queue attribute that can be used in SQS.createQueue
 */
trait CreateAttributeValue extends QueueAttributeValue

case class ApproximateNumberOfMessages(count: Long) extends QueueAttributeValue {
  override def attribute = QueueAttributes.ApproximateNumberOfMessages
  override def value = count.toString
}

case class ApproximateNumberOfMessagesDelayed(count: Long) extends QueueAttributeValue {
  override def attribute = QueueAttributes.ApproximateNumberOfMessagesDelayed
  override def value = count.toString
}

case class ApproximateNumberOfMessagesNotVisible(count: Long) extends QueueAttributeValue {
  override def attribute = QueueAttributes.ApproximateNumberOfMessagesNotVisible
  override def value = count.toString
}

case class CreatedTimestamp(timestamp: Long) extends QueueAttributeValue {
  override def attribute = QueueAttributes.CreatedTimestamp
  override def value = timestamp.toString
}

case class DelaySeconds(seconds: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttributes.DelaySeconds
  override def value = seconds.toString
}

// LastModifiedTimestamp

case class MaximumMessageSize(size: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttributes.MaximumMessageSize
  override def value = size.toString
}

case class MessageRetentionPeriod(period: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttributes.MessageRetentionPeriod
  override def value = period.toString
}

case class Policy(policy: JsValue) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttributes.Policy
  override def value = policy.toString
}

case class QueueArn(arn: String) extends QueueAttributeValue {
  override def attribute = QueueAttributes.QueueArn
  override def value = arn
}

case class ReceiveMessageWaitTimeSeconds(waitTime: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttributes.ReceiveMessageWaitTimeSeconds
  override def value = waitTime.toString
}

case class VisibilityTimeout(timeout: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttributes.VisibilityTimeout
  override def value = timeout.toString
}

object QueueAttributeValue {
  def apply(name: String, value: String): QueueAttributeValue = name match {
    case "ApproximateNumberOfMessages" => ApproximateNumberOfMessages(value.toLong)
    case "ApproximateNumberOfMessagesDelayed" => ApproximateNumberOfMessagesDelayed(value.toLong)
    case "ApproximateNumberOfMessagesNotVisible" => ApproximateNumberOfMessagesNotVisible(value.toLong)
    case "VisibilityTimeout" => VisibilityTimeout(value.toLong)
    case "Policy" => Policy(Json.parse(value))
    case "MaximumMessageSize" => MaximumMessageSize(value.toLong)
    case "MessageRetentionPeriod" => MessageRetentionPeriod(value.toLong)
    case "DelaySeconds" => DelaySeconds(value.toLong)
  }

}
