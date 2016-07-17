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
package cloudwatch

import scala.collection.JavaConverters._
import scala.concurrent.Future

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient
import com.amazonaws.services.cloudwatch.model._

/**
 * A lightweight wrapper for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonCloudWatchAsyncClient]].
 *
 * @constructor construct a wrapper client from an Amazon async client.
 * @param client
  *     the underlying [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonCloudWatchAsyncClient]].
 * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonCloudWatchAsyncClient]]
 */
class AmazonCloudWatchScalaClient(val client: AmazonCloudWatchAsyncClient) {

/**
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#deleteAlarms(com.amazonaws.services.cloudwatch.model.DeleteAlarmsRequest) AWS Java SDK]]
  */
  def deleteAlarms(
    deleteAlarmsRequest: DeleteAlarmsRequest
  ): Future[DeleteAlarmsResult] =
    wrapAsyncMethod[DeleteAlarmsRequest, DeleteAlarmsResult](client.deleteAlarmsAsync, deleteAlarmsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#deleteAlarms(com.amazonaws.services.cloudwatch.model.DeleteAlarmsRequest) AWS Java SDK]]
    */
  def deleteAlarms(alarmNames: String*): Future[DeleteAlarmsResult] =
    deleteAlarms(new DeleteAlarmsRequest().withAlarmNames(alarmNames: _*))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarmHistory(com.amazonaws.services.cloudwatch.model.DescribeAlarmHistoryRequest) AWS Java SDK]]
    */
  def describeAlarmHistory(
    describeAlarmHistoryRequest: DescribeAlarmHistoryRequest
  ): Future[DescribeAlarmHistoryResult] =
    wrapAsyncMethod[DescribeAlarmHistoryRequest, DescribeAlarmHistoryResult](client.describeAlarmHistoryAsync, describeAlarmHistoryRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarmHistory() AWS Java SDK]]
    */
  def describeAlarmHistory(): Future[DescribeAlarmHistoryResult] =
    describeAlarmHistory(new DescribeAlarmHistoryRequest())

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarms(com.amazonaws.services.cloudwatch.model.DescribeAlarmsRequest) AWS Java SDK]]
    */
  def describeAlarms(
    describeAlarmRequest: DescribeAlarmsRequest
  ): Future[DescribeAlarmsResult] =
    wrapAsyncMethod[DescribeAlarmsRequest, DescribeAlarmsResult](client.describeAlarmsAsync, describeAlarmRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarms() AWS Java SDK]]
    */
  def describeAlarms(): Future[DescribeAlarmsResult] =
    describeAlarms(new DescribeAlarmsRequest())

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#describeAlarmsForMetric(com.amazonaws.services.cloudwatch.model.DescribeAlarmsForMetricRequest) AWS Java SDK]]
    */
  def describeAlarmsForMetric(
    describeAlarmsForMetricRequest: DescribeAlarmsForMetricRequest
  ): Future[DescribeAlarmsForMetricResult] =
    wrapAsyncMethod[DescribeAlarmsForMetricRequest, DescribeAlarmsForMetricResult](client.describeAlarmsForMetricAsync, describeAlarmsForMetricRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#disableAlarmActions(com.amazonaws.services.cloudwatch.model.DisableAlarmActionsRequest) AWS Java SDK]]
    */
  def disableAlarmActions(
    disableAlarmActionsRequest: DisableAlarmActionsRequest
  ): Future[DisableAlarmActionsResult] =
    wrapAsyncMethod[DisableAlarmActionsRequest, DisableAlarmActionsResult](client.disableAlarmActionsAsync, disableAlarmActionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#disableAlarmActions(com.amazonaws.services.cloudwatch.model.DisableAlarmActionsRequest) AWS Java SDK]]
    */
  def disableAlarmActions(alarmNames: String*): Future[DisableAlarmActionsResult] =
    disableAlarmActions(new DisableAlarmActionsRequest().withAlarmNames(alarmNames: _*))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#enableAlarmActions(com.amazonaws.services.cloudwatch.model.EnableAlarmActionsRequest) AWS Java SDK]]
    */
  def enableAlarmActions(
    enableAlarmActionsRequest: EnableAlarmActionsRequest
  ): Future[EnableAlarmActionsResult] =
    wrapAsyncMethod[EnableAlarmActionsRequest, EnableAlarmActionsResult](client.enableAlarmActionsAsync, enableAlarmActionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#enableAlarmActions(com.amazonaws.services.cloudwatch.model.EnableAlarmActionsRequest) AWS Java SDK]]
    */
  def enableAlarmActions(alarmNames: String*): Future[EnableAlarmActionsResult] =
    enableAlarmActions(new EnableAlarmActionsRequest().withAlarmNames(alarmNames: _*))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#getMetricStatistics(com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest) AWS Java SDK]]
    */
  def getMetricStatistics(
    getMetricStatisticsRequest: GetMetricStatisticsRequest
  ): Future[GetMetricStatisticsResult] =
    wrapAsyncMethod[GetMetricStatisticsRequest, GetMetricStatisticsResult](client.getMetricStatisticsAsync, getMetricStatisticsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#listMetrics(com.amazonaws.services.cloudwatch.model.ListMetricsRequest) AWS Java SDK]]
    */
  def listMetrics(
    listMetricsRequest: ListMetricsRequest
  ): Future[ListMetricsResult] =
    wrapAsyncMethod[ListMetricsRequest, ListMetricsResult](client.listMetricsAsync, listMetricsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#listMetrics() AWS Java SDK]]
    */
  def listMetrics(): Future[ListMetricsResult] =
    listMetrics(new ListMetricsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#putMetricAlarm(com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest) AWS Java SDK]]
    */
  def putMetricAlarm(
    putMetricAlarmRequest: PutMetricAlarmRequest
  ): Future[PutMetricAlarmResult] =
    wrapAsyncMethod[PutMetricAlarmRequest, PutMetricAlarmResult](client.putMetricAlarmAsync, putMetricAlarmRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#putMetricData(com.amazonaws.services.cloudwatch.model.PutMetricDataRequest) AWS Java SDK]]
    */
  def putMetricData(
    putMetricDataRequest: PutMetricDataRequest
  ): Future[PutMetricDataResult] =
    wrapAsyncMethod[PutMetricDataRequest, PutMetricDataResult](client.putMetricDataAsync, putMetricDataRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#putMetricData(com.amazonaws.services.cloudwatch.model.PutMetricDataRequest) AWS Java SDK]]
    */
  def putMetricData(
    namespace:  String,
    metricData: Iterable[MetricDatum]
  ): Future[PutMetricDataResult] =
    putMetricData(
      new PutMetricDataRequest()
      .withNamespace(namespace)
      .withMetricData(metricData.asJavaCollection)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#setAlarmState(com.amazonaws.services.cloudwatch.model.SetAlarmStateRequest) AWS Java SDK]]
    */
  def setAlarmState(
    setAlarmStateRequest: SetAlarmStateRequest
  ): Future[SetAlarmStateResult] =
    wrapAsyncMethod[SetAlarmStateRequest, SetAlarmStateResult](client.setAlarmStateAsync, setAlarmStateRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#setAlarmState(com.amazonaws.services.cloudwatch.model.SetAlarmStateRequest) AWS Java SDK]]
    */
  def setAlarmState(
    alarmName: String,
    stateReason: String,
    stateValue: StateValue,
    stateReasonData: String = ""
  ): Future[SetAlarmStateResult] =
    setAlarmState(
      new SetAlarmStateRequest()
      .withAlarmName(alarmName)
      .withStateReason(stateReason)
      .withStateValue(stateValue)
      .withStateReasonData(stateReasonData)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatch.html#shutdown() AWS Java SDK]]
    */
  def shutdown(): Unit =
    client.shutdown()

}
