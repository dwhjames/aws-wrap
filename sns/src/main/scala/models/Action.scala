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

package aws.sns

sealed trait Action

object Action {

  case object AddPermission extends Action {
    override def toString = "AddPermission"
  }

  case object ConfirmSubscription extends Action {
    override def toString = "ConfirmSubscription"
  }

  case object CreateTopic extends Action {
    override def toString = "CreateTopic"
  }

  case object DeleteTopic extends Action {
    override def toString = "DeleteTopic"
  }

  case object GetSubscriptionAttributes extends Action {
    override def toString = "GetSubscriptionAttributes"
  }

  case object GetTopicAttributes extends Action {
    override def toString = "GetTopicAttributes"
  }

  case object ListSubscriptions extends Action {
    override def toString = "ListSubscriptions"
  }

  case object ListSubscriptionsByTopic extends Action {
    override def toString = "ListSubscriptionsByTopic"
  }

  case object ListTopics extends Action {
    override def toString = "ListTopics"
  }

  case object Publish extends Action {
    override def toString = "Publish"
  }

  case object RemovePermission extends Action {
    override def toString = "RemovePermission"
  }

  case object SetSubscriptionAttributes extends Action {
    override def toString = "SetSubscriptionAttributes"
  }

  case object SetTopicAttributes extends Action {
    override def toString = "SetTopicAttributes"
  }

  case object Subscribe extends Action {
    override def toString = "Subscribe"
  }

  case object Unsubscribe extends Action {
    override def toString = "Unsubscribe"
  }

}

