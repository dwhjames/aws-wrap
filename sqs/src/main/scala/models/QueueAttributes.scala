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
