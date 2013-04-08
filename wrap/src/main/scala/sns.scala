
package aws.wrap

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.sns._
import com.amazonaws.services.sns.model._

trait AmazonSNSScalaClient {

  val client: AmazonSNSAsyncClient

  def addPermissionAsync(
    addPermissionAsyncRequest: AddPermissionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.addPermissionAsync, addPermissionAsyncRequest)

  def confirmSubscription(
    confirmSubscriptionRequest: ConfirmSubscriptionRequest
  ): Future[ConfirmSubscriptionResult] =
    wrapAsyncMethod(client.confirmSubscriptionAsync, confirmSubscriptionRequest)

  def confirmSubscription(
    topicArn: String,
    token:    String
  ): Future[ConfirmSubscriptionResult] =
    confirmSubscription(new ConfirmSubscriptionRequest(topicArn, token))

  def createTopic(
    createTopicRequest: CreateTopicRequest
  ): Future[CreateTopicResult] =
    wrapAsyncMethod(client.createTopicAsync, createTopicRequest)

  def createTopic(
    name: String
  ): Future[CreateTopicResult] =
    createTopic(new CreateTopicRequest(name))

  def deleteTopic(
    deleteTopicRequest: DeleteTopicRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteTopicAsync, deleteTopicRequest)

  def deleteTopic(
    topicArn: String
  ): Future[Unit] =
    deleteTopic(new DeleteTopicRequest(topicArn))

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

  def getExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(client.getExecutorService())

  def getSubscriptionAttributes(
    getSubscriptionAttributesRequest: GetSubscriptionAttributesRequest
  ): Future[GetSubscriptionAttributesResult] =
    wrapAsyncMethod(client.getSubscriptionAttributesAsync, getSubscriptionAttributesRequest)

  def getSubscriptionAttributes(
    subsciptionArn: String
  ): Future[GetSubscriptionAttributesResult] =
    getSubscriptionAttributes(new GetSubscriptionAttributesRequest(subsciptionArn))

  def getTopicAttributes(getTopicAttributesRequest: GetTopicAttributesRequest): Future[GetTopicAttributesResult] =
    wrapAsyncMethod(client.getTopicAttributesAsync, getTopicAttributesRequest)

  def getTopicAttributes(
    topicArn: String
  ): Future[GetTopicAttributesResult] =
    getTopicAttributes(new GetTopicAttributesRequest(topicArn))

  def listSubscriptions(
    listSubscriptionsRequest: ListSubscriptionsRequest
  ): Future[ListSubscriptionsResult] =
    wrapAsyncMethod(client.listSubscriptionsAsync, listSubscriptionsRequest)

  def listSubscriptions(
    nextToken: String = null
  ): Future[ListSubscriptionsResult] =
    listSubscriptions(new ListSubscriptionsRequest(nextToken))

  def listSubscriptionsByTopic(
    listSubscriptionsByTopicRequest: ListSubscriptionsByTopicRequest
  ): Future[ListSubscriptionsByTopicResult] =
      wrapAsyncMethod(client.listSubscriptionsByTopicAsync, listSubscriptionsByTopicRequest)

  def listSubscriptionsByTopic(
    topicArn:  String,
    nextToken: String = null
  ): Future[ListSubscriptionsByTopicResult] =
    listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest(topicArn, nextToken))

  def listTopics(
    listTopicsRequest: ListTopicsRequest
  ): Future[ListTopicsResult] =
    wrapAsyncMethod(client.listTopicsAsync, listTopicsRequest)

  def listTopics(
    nextToken: String = null
  ): Future[ListTopicsResult] =
    listTopics(new ListTopicsRequest(nextToken))

  def publish(
    publishRequest: PublishRequest
  ): Future[PublishResult] =
    wrapAsyncMethod(client.publishAsync, publishRequest)

  def publish(
    topicArn: String,
    message:  String
  ): Future[PublishResult] =
    publish(new PublishRequest(topicArn, message))

  def removePermission(
    removePermissionRequest: RemovePermissionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.removePermissionAsync, removePermissionRequest)

  def setSubscriptionAttributes(
    setSubscriptionAttributesRequest: SetSubscriptionAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.setSubscriptionAttributesAsync, setSubscriptionAttributesRequest)

  def setTopicAttributes(
    setTopicAttributesRequest: SetTopicAttributesRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.setTopicAttributesAsync, setTopicAttributesRequest)

  def shutdown(): Unit =
    client.shutdown()

  def subscribe(
    subscribeRequest: SubscribeRequest
  ): Future[SubscribeResult] =
    wrapAsyncMethod(client.subscribeAsync, subscribeRequest)

  def subscribe(
    topicArn: String,
    protocol: String,
    endpoint: String
  ): Future[SubscribeResult] =
    subscribe(new SubscribeRequest(topicArn, protocol, endpoint))

  def unsubscribe(
    unsubscribeRequest: UnsubscribeRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.unsubscribeAsync, unsubscribeRequest)

  def unsubscribe(
    subsciptionArn: String
  ): Future[Unit] =
    unsubscribe(new UnsubscribeRequest(subsciptionArn))

}

object AmazonSNSScalaClient {

  private class AmazonSNSScalaClientImpl(override val client: AmazonSNSAsyncClient) extends AmazonSNSScalaClient

  def fromAsyncClient(client: AmazonSNSAsyncClient): AmazonSNSScalaClient =
    new AmazonSNSScalaClientImpl(client)

}
