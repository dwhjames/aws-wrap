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

sealed trait QueueAttribute

object QueueAttribute {
  case object All extends QueueAttribute {
    override def toString() = "All"
  }
  case object ApproximateNumberOfMessages extends QueueAttribute {
    override def toString() = "ApproximateNumberOfMessages"
  }
  case object ApproximateNumberOfMessagesDelayed extends QueueAttribute {
    override def toString() = "ApproximateNumberOfMessagesDelayed"
  }
  case object ApproximateNumberOfMessagesNotVisible extends QueueAttribute {
    override def toString() = "ApproximateNumberOfMessagesNotVisible"
  }
  case object CreatedTimestamp extends QueueAttribute {
    override def toString() = "CreatedTimestamp"
  }
  case object DelaySeconds extends QueueAttribute {
    override def toString() = "DelaySeconds"
  }
  case object LastModifiedTimestamp extends QueueAttribute {
    override def toString() = "LastModifiedTimestamp"
  }
  case object MaximumMessageSize extends QueueAttribute {
    override def toString() = "MaximumMessageSize"
  }
  case object MessageRetentionPeriod extends QueueAttribute {
    override def toString() = "MessageRetentionPeriod"
  }
  case object OldestMessageAge extends QueueAttribute {
    override def toString() = "OldestMessageAge"
  }
  case object Policy extends QueueAttribute {
    override def toString() = "Policy"
  }
  case object QueueArn extends QueueAttribute {
    override def toString() = "QueueArn"
  }
  case object ReceiveMessageWaitTimeSeconds extends QueueAttribute {
    override def toString() = "ReceiveMessageWaitTimeSeconds"
  }
  case object VisibilityTimeout extends QueueAttribute {
    override def toString() = "VisibilityTimeout"
  }
  def apply(attribute: String): QueueAttribute = attribute match {
    case "All" => All
    case "ApproximateNumberOfMessages" => ApproximateNumberOfMessages
    case "ApproximateNumberOfMessagesDelayed" => ApproximateNumberOfMessagesDelayed
    case "ApproximateNumberOfMessagesNotVisible" => ApproximateNumberOfMessagesNotVisible
    case "CreatedTimestamp" => CreatedTimestamp
    case "DelaySeconds" => DelaySeconds
    case "LastModifiedTimestamp" => LastModifiedTimestamp
    case "MaximumMessageSize" => MaximumMessageSize
    case "MessageRetentionPeriod" => MessageRetentionPeriod
    case "OldestMessageAge" => OldestMessageAge
    case "Policy" => Policy
    case "QueueArn" => QueueArn
    case "ReceiveMessageWaitTimeSeconds" => ReceiveMessageWaitTimeSeconds
    case "VisibilityTimeout" => VisibilityTimeout
  }
}

trait QueueAttributeValue {
  def attribute: QueueAttribute
  def value: String
}

/**
 * A queue attribute that can be used in SQS.createQueue
 */
trait CreateAttributeValue extends QueueAttributeValue

case class ApproximateNumberOfMessages(count: Long) extends QueueAttributeValue {
  override def attribute = QueueAttribute.ApproximateNumberOfMessages
  override def value = count.toString
}

case class ApproximateNumberOfMessagesDelayed(count: Long) extends QueueAttributeValue {
  override def attribute = QueueAttribute.ApproximateNumberOfMessagesDelayed
  override def value = count.toString
}

case class ApproximateNumberOfMessagesNotVisible(count: Long) extends QueueAttributeValue {
  override def attribute = QueueAttribute.ApproximateNumberOfMessagesNotVisible
  override def value = count.toString
}

case class CreatedTimestamp(timestamp: Long) extends QueueAttributeValue {
  override def attribute = QueueAttribute.CreatedTimestamp
  override def value = timestamp.toString
}

case class DelaySeconds(seconds: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttribute.DelaySeconds
  override def value = seconds.toString
}

// LastModifiedTimestamp

case class MaximumMessageSize(size: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttribute.MaximumMessageSize
  override def value = size.toString
}

case class MessageRetentionPeriod(period: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttribute.MessageRetentionPeriod
  override def value = period.toString
}

case class Policy(policy: JsValue) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttribute.Policy
  override def value = policy.toString
}

case class QueueArn(arn: String) extends QueueAttributeValue {
  override def attribute = QueueAttribute.QueueArn
  override def value = arn
}

case class ReceiveMessageWaitTimeSeconds(waitTime: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttribute.ReceiveMessageWaitTimeSeconds
  override def value = waitTime.toString
}

case class VisibilityTimeout(timeout: Long) extends QueueAttributeValue with CreateAttributeValue {
  override def attribute = QueueAttribute.VisibilityTimeout
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
