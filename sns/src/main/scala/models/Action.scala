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

