/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
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

package com.github.dwhjames.awswrap
package sns

import scala.concurrent.{Future, ExecutionContext}

import java.util.concurrent.ExecutorService

import com.amazonaws.services.sns._
import com.amazonaws.services.sns.model._

class AmazonSNSScalaClient(val client: AmazonSNSAsyncClient) {

  def addPermissionAsync(
    addPermissionAsyncRequest: AddPermissionRequest
  ): Future[AddPermissionResult] =
    wrapAsyncMethod[AddPermissionRequest, AddPermissionResult](client.addPermissionAsync, addPermissionAsyncRequest)

  def confirmSubscription(
    confirmSubscriptionRequest: ConfirmSubscriptionRequest
  ): Future[ConfirmSubscriptionResult] =
    wrapAsyncMethod[ConfirmSubscriptionRequest, ConfirmSubscriptionResult](client.confirmSubscriptionAsync, confirmSubscriptionRequest)

  def confirmSubscription(
    topicArn: String,
    token:    String
  ): Future[ConfirmSubscriptionResult] =
    confirmSubscription(new ConfirmSubscriptionRequest(topicArn, token))

  def createTopic(
    createTopicRequest: CreateTopicRequest
  ): Future[CreateTopicResult] =
    wrapAsyncMethod[CreateTopicRequest, CreateTopicResult](client.createTopicAsync, createTopicRequest)

  def createTopic(
    name: String
  ): Future[CreateTopicResult] =
    createTopic(new CreateTopicRequest(name))

  def deleteTopic(
    deleteTopicRequest: DeleteTopicRequest
  ): Future[DeleteTopicResult] =
    wrapAsyncMethod[DeleteTopicRequest, DeleteTopicResult](client.deleteTopicAsync, deleteTopicRequest)

  def deleteTopic(
    topicArn: String
  ): Future[DeleteTopicResult] =
    deleteTopic(new DeleteTopicRequest(topicArn))

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

  def getExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(client.getExecutorService())

  def getSubscriptionAttributes(
    getSubscriptionAttributesRequest: GetSubscriptionAttributesRequest
  ): Future[GetSubscriptionAttributesResult] =
    wrapAsyncMethod[GetSubscriptionAttributesRequest, GetSubscriptionAttributesResult](client.getSubscriptionAttributesAsync, getSubscriptionAttributesRequest)

  def getSubscriptionAttributes(
    subsciptionArn: String
  ): Future[GetSubscriptionAttributesResult] =
    getSubscriptionAttributes(new GetSubscriptionAttributesRequest(subsciptionArn))

  def getTopicAttributes(getTopicAttributesRequest: GetTopicAttributesRequest): Future[GetTopicAttributesResult] =
    wrapAsyncMethod[GetTopicAttributesRequest, GetTopicAttributesResult](client.getTopicAttributesAsync, getTopicAttributesRequest)

  def getTopicAttributes(
    topicArn: String
  ): Future[GetTopicAttributesResult] =
    getTopicAttributes(new GetTopicAttributesRequest(topicArn))

  def listSubscriptions(
    listSubscriptionsRequest: ListSubscriptionsRequest
  ): Future[ListSubscriptionsResult] =
    wrapAsyncMethod[ListSubscriptionsRequest, ListSubscriptionsResult](client.listSubscriptionsAsync, listSubscriptionsRequest)

  def listSubscriptions(
    nextToken: String = null
  ): Future[ListSubscriptionsResult] =
    listSubscriptions(new ListSubscriptionsRequest(nextToken))

  def listSubscriptionsByTopic(
    listSubscriptionsByTopicRequest: ListSubscriptionsByTopicRequest
  ): Future[ListSubscriptionsByTopicResult] =
      wrapAsyncMethod[ListSubscriptionsByTopicRequest, ListSubscriptionsByTopicResult](client.listSubscriptionsByTopicAsync, listSubscriptionsByTopicRequest)

  def listSubscriptionsByTopic(
    topicArn:  String,
    nextToken: String = null
  ): Future[ListSubscriptionsByTopicResult] =
    listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest(topicArn, nextToken))

  def listTopics(
    listTopicsRequest: ListTopicsRequest
  ): Future[ListTopicsResult] =
    wrapAsyncMethod[ListTopicsRequest, ListTopicsResult](client.listTopicsAsync, listTopicsRequest)

  def listTopics(
    nextToken: String = null
  ): Future[ListTopicsResult] =
    listTopics(new ListTopicsRequest(nextToken))

  def publish(
    publishRequest: PublishRequest
  ): Future[PublishResult] =
    wrapAsyncMethod[PublishRequest, PublishResult](client.publishAsync, publishRequest)

  def publish(
    topicArn: String,
    message:  String
  ): Future[PublishResult] =
    publish(new PublishRequest(topicArn, message))

  def removePermission(
    removePermissionRequest: RemovePermissionRequest
  ): Future[RemovePermissionResult] =
    wrapAsyncMethod[RemovePermissionRequest, RemovePermissionResult](client.removePermissionAsync, removePermissionRequest)

  def setSubscriptionAttributes(
    setSubscriptionAttributesRequest: SetSubscriptionAttributesRequest
  ): Future[SetSubscriptionAttributesResult] =
    wrapAsyncMethod[SetSubscriptionAttributesRequest, SetSubscriptionAttributesResult](client.setSubscriptionAttributesAsync, setSubscriptionAttributesRequest)

  def setTopicAttributes(
    setTopicAttributesRequest: SetTopicAttributesRequest
  ): Future[SetTopicAttributesResult] =
    wrapAsyncMethod[SetTopicAttributesRequest, SetTopicAttributesResult](client.setTopicAttributesAsync, setTopicAttributesRequest)

  def shutdown(): Unit =
    client.shutdown()

  def subscribe(
    subscribeRequest: SubscribeRequest
  ): Future[SubscribeResult] =
    wrapAsyncMethod[SubscribeRequest, SubscribeResult](client.subscribeAsync, subscribeRequest)

  def subscribe(
    topicArn: String,
    protocol: String,
    endpoint: String
  ): Future[SubscribeResult] =
    subscribe(new SubscribeRequest(topicArn, protocol, endpoint))

  def unsubscribe(
    unsubscribeRequest: UnsubscribeRequest
  ): Future[UnsubscribeResult] =
    wrapAsyncMethod[UnsubscribeRequest, UnsubscribeResult](client.unsubscribeAsync, unsubscribeRequest)

  def unsubscribe(
    subsciptionArn: String
  ): Future[UnsubscribeResult] =
    unsubscribe(new UnsubscribeRequest(subsciptionArn))

  def createPlatformEndpoint(
    createPlatformEndpointRequest: CreatePlatformEndpointRequest
  ): Future[CreatePlatformEndpointResult] =
    wrapAsyncMethod[CreatePlatformEndpointRequest, CreatePlatformEndpointResult](client.createPlatformEndpointAsync, createPlatformEndpointRequest)

  def setEndpointAttributes(
    setEndpointAttributesRequest: SetEndpointAttributesRequest
  ): Future[SetEndpointAttributesResult] =
    wrapAsyncMethod[SetEndpointAttributesRequest, SetEndpointAttributesResult](client.setEndpointAttributesAsync, setEndpointAttributesRequest)

  def getEndpointAttributes(
    getEndpointAttributesRequest: GetEndpointAttributesRequest
  ): Future[GetEndpointAttributesResult] =
    wrapAsyncMethod[GetEndpointAttributesRequest, GetEndpointAttributesResult](client.getEndpointAttributesAsync, getEndpointAttributesRequest)

}
