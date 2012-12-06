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

  object AddPermission extends Action {
    override def toString = "AddPermission"
  }

  object ConfirmSubscription extends Action {
    override def toString = "ConfirmSubscription"
  }

  object CreateTopic extends Action {
    override def toString = "CreateTopic"
  }

  object DeleteTopic extends Action {
    override def toString = "DeleteTopic"
  }

  object GetSubscriptionAttributes extends Action {
    override def toString = "GetSubscriptionAttributes"
  }

  object GetTopicAttributes extends Action {
    override def toString = "GetTopicAttributes"
  }

  object ListSubscriptions extends Action {
    override def toString = "ListSubscriptions"
  }

  object ListSubscriptionsByTopic extends Action {
    override def toString = "ListSubscriptionsByTopic"
  }

  object ListTopics extends Action {
    override def toString = "ListTopics"
  }

  object Publish extends Action {
    override def toString = "Publish"
  }

  object RemovePermission extends Action {
    override def toString = "RemovePermission"
  }

  object SetSubscriptionAttributes extends Action {
    override def toString = "SetSubscriptionAttributes"
  }

  object SetTopicAttributes extends Action {
    override def toString = "SetTopicAttributes"
  }

  object Subscribe extends Action {
    override def toString = "Subscribe"
  }

  object Unsubscribe extends Action {
    override def toString = "Unsubscribe"
  }

  def apply(name: String) = name match {
    case "AddPermission" => AddPermission
    case "ConfirmSubscription" => ConfirmSubscription
    case "CreateTopic" => CreateTopic
    case "DeleteTopic" => DeleteTopic
    case "GetSubscriptionAttributes" => GetSubscriptionAttributes
    case "GetTopicAttributes" => GetTopicAttributes
    case "ListSubscriptions" => ListSubscriptions
    case "ListSubscriptionsByTopic" => ListSubscriptionsByTopic
    case "ListTopics" => ListTopics
    case "Publish" => Publish
    case "RemovePermission" => RemovePermission
    case "SetSubscriptionAttributes" => SetSubscriptionAttributes
    case "SetTopicAttributes" => SetTopicAttributes
    case "Subscribe" => Subscribe
    case "Unsubscribe" => Unsubscribe
    case _ => sys.error("Unkown action: " + name)
  }

}

