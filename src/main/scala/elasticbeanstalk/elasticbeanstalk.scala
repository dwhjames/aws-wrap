/*
 * Copyright 2015 Daniel W. H. James
 * Copyright 2015 Andrea Lattuada
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
package elasticbeanstalk

// import scala.collection.JavaConverters._
import scala.concurrent.Future

import com.amazonaws.services.elasticbeanstalk.AWSElasticBeanstalkAsyncClient
import com.amazonaws.services.elasticbeanstalk.model._


/**
 * A lightweight wrapper for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalkAsyncClient.html AWSElasticBeanstalkAsyncClient]].
 *
 * @constructor construct a wrapper client from an Amazon async client.
 * @param client
  *     the underlying [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalkAsyncClient.html AWSElasticBeanstalkAsyncClient]].
 * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalkAsyncClient.html AWSElasticBeanstalkAsyncClient]]
 */
class AWSElasticBeanstalkScalaClient(val client: AWSElasticBeanstalkAsyncClient) {

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#abortEnvironmentUpdate(com.amazonaws.services.elasticbeanstalk.model.AbortEnvironmentUpdateRequest) AWS Java SDK]]
    */
  def abortEnvironmentUpdate(
    abortEnvironmentUpdateRequest: AbortEnvironmentUpdateRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.abortEnvironmentUpdateAsync, abortEnvironmentUpdateRequest)

  /**
    * @param cnamePrefix The prefix used when this CNAME is checked.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#checkDNSAvailability(com.amazonaws.services.elasticbeanstalk.model.CheckDNSAvailabilityRequest) AWS Java SDK]]
    */
  def checkDNSAvailability(cnamePrefix: String): Future[CheckDNSAvailabilityResult] =
    checkDNSAvailability(new CheckDNSAvailabilityRequest(cnamePrefix))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#checkDNSAvailability(com.amazonaws.services.elasticbeanstalk.model.CheckDNSAvailabilityRequest) AWS Java SDK]]
    */
  def checkDNSAvailability(
    checkDNSAvailabilityRequest: CheckDNSAvailabilityRequest
  ): Future[CheckDNSAvailabilityResult] =
    wrapAsyncMethod(client.checkDNSAvailabilityAsync, checkDNSAvailabilityRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#createApplication(com.amazonaws.services.elasticbeanstalk.model.CreateApplicationRequest) AWS Java SDK]]
    */
  def createApplication(
    createApplicationRequest: CreateApplicationRequest
  ): Future[CreateApplicationResult] =
    wrapAsyncMethod(client.createApplicationAsync, createApplicationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#createEnvironment(com.amazonaws.services.elasticbeanstalk.model.CreateEnvironmentRequest) AWS Java SDK]]
    */
  def createEnvironment(
    createEnvironmentRequest: CreateEnvironmentRequest
  ): Future[CreateEnvironmentResult] =
    wrapAsyncMethod(client.createEnvironmentAsync, createEnvironmentRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#createStorageLocation(com.amazonaws.services.elasticbeanstalk.model.CreateStorageLocationRequest) AWS Java SDK]]
    */
  def createStorageLocation(
    createStorageLocationRequest: CreateStorageLocationRequest
  ): Future[CreateStorageLocationResult] =
    wrapAsyncMethod(client.createStorageLocationAsync, createStorageLocationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#createStorageLocation() AWS Java SDK]]
    */
  def createStorageLocation(): Future[CreateStorageLocationResult] =
    createStorageLocation(new CreateStorageLocationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#deleteApplication(com.amazonaws.services.elasticbeanstalk.model.DeleteApplicationRequest) AWS Java SDK]]
    */
  def deleteApplication(
    deleteApplicationRequest: DeleteApplicationRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteApplicationAsync, deleteApplicationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#deleteApplicationVersion(com.amazonaws.services.elasticbeanstalk.model.DeleteApplicationVersionRequest) AWS Java SDK]]
    */
  def deleteApplicationVersion(
    deleteApplicationVersionRequest: DeleteApplicationVersionRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteApplicationVersionAsync, deleteApplicationVersionRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#deleteEnvironmentConfiguration(com.amazonaws.services.elasticbeanstalk.model.DeleteEnvironmentConfigurationRequest) AWS Java SDK]]
    */
  def deleteEnvironmentConfiguration(
    deleteEnvironmentConfigurationRequest: DeleteEnvironmentConfigurationRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteEnvironmentConfigurationAsync, deleteEnvironmentConfigurationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeApplications(com.amazonaws.services.elasticbeanstalk.model.DescribeApplicationsRequest) AWS Java SDK]]
    */
  def describeApplications(
    describeApplicationsRequest: DescribeApplicationsRequest
  ): Future[DescribeApplicationsResult] =
    wrapAsyncMethod(client.describeApplicationsAsync, describeApplicationsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeApplications() AWS Java SDK]]
    */
  def describeApplications(): Future[DescribeApplicationsResult] =
    describeApplications(new DescribeApplicationsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeApplicationVersions(com.amazonaws.services.elasticbeanstalk.model.DescribeApplicationVersionsRequest) AWS Java SDK]]
    */
  def describeApplicationVersions(
    describeApplicationVersionsRequest: DescribeApplicationVersionsRequest
  ): Future[DescribeApplicationVersionsResult] =
    wrapAsyncMethod(client.describeApplicationVersionsAsync, describeApplicationVersionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeApplicationVersions() AWS Java SDK]]
    */
  def describeApplicationVersions(applicationName: String): Future[DescribeApplicationVersionsResult] =
    describeApplicationVersions(new DescribeApplicationVersionsRequest().withApplicationName(applicationName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeConfigurationOptions(com.amazonaws.services.elasticbeanstalk.model.DescribeConfigurationOptionsRequest) AWS Java SDK]]
    */
  def describeConfigurationOptions(
    describeConfigurationOptionsRequest: DescribeConfigurationOptionsRequest
  ): Future[DescribeConfigurationOptionsResult] =
    wrapAsyncMethod(client.describeConfigurationOptionsAsync, describeConfigurationOptionsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeConfigurationSettings(com.amazonaws.services.elasticbeanstalk.model.DescribeConfigurationSettingsRequest) AWS Java SDK]]
    */
  def describeConfigurationSettings(
    describeConfigurationSettingsRequest: DescribeConfigurationSettingsRequest
  ): Future[DescribeConfigurationSettingsResult] =
    wrapAsyncMethod(client.describeConfigurationSettingsAsync, describeConfigurationSettingsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeEnvironmentResources(com.amazonaws.services.elasticbeanstalk.model.DescribeEnvironmentResourcesRequest) AWS Java SDK]]
    */
  def describeEnvironmentResources(
    describeEnvironmentResourcesRequest: DescribeEnvironmentResourcesRequest
  ): Future[DescribeEnvironmentResourcesResult] =
    wrapAsyncMethod(client.describeEnvironmentResourcesAsync, describeEnvironmentResourcesRequest)

  /**
    * @param environmentName The name of the environment to retrieve AWS resource usage data.
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeEnvironmentResources() AWS Java SDK]]
    */
  def describeEnvironmentResources(environmentName: String): Future[DescribeEnvironmentResourcesResult] =
    describeEnvironmentResources(new DescribeEnvironmentResourcesRequest().withEnvironmentName(environmentName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeEvents(com.amazonaws.services.elasticbeanstalk.model.DescribeEventsRequest) AWS Java SDK]]
    */
  def describeEvents(
    describeEventsRequest: DescribeEventsRequest
  ): Future[DescribeEventsResult] =
    wrapAsyncMethod(client.describeEventsAsync, describeEventsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeEnvironments(com.amazonaws.services.elasticbeanstalk.model.DescribeEnvironmentsRequest) AWS Java SDK]]
    */
  def describeEnvironments(
    describeEnvironmentsRequest: DescribeEnvironmentsRequest
  ): Future[DescribeEnvironmentsResult] =
    wrapAsyncMethod(client.describeEnvironmentsAsync, describeEnvironmentsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#describeEnvironments() AWS Java SDK]]
    */
  def describeEnvironments(): Future[DescribeEnvironmentsResult] =
    describeEnvironments(new DescribeEnvironmentsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#rebuildEnvironment(com.amazonaws.services.elasticbeanstalk.model.RebuildEnvironmentRequest) AWS Java SDK]]
    */
  def rebuildEnvironment(
    rebuildEnvironmentRequest: RebuildEnvironmentRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.rebuildEnvironmentAsync, rebuildEnvironmentRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#restartAppServer(com.amazonaws.services.elasticbeanstalk.model.RestartAppServerRequest) AWS Java SDK]]
    */
  def restartAppServer(
    restartAppServerRequest: RestartAppServerRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.restartAppServerAsync, restartAppServerRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#updateApplication(com.amazonaws.services.elasticbeanstalk.model.UpdateApplicationRequest) AWS Java SDK]]
    */
  def updateApplication(
    updateApplicationRequest: UpdateApplicationRequest
  ): Future[UpdateApplicationResult] =
    wrapAsyncMethod(client.updateApplicationAsync, updateApplicationRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#updateApplicationVersion(com.amazonaws.services.elasticbeanstalk.model.UpdateApplicationVersionRequest) AWS Java SDK]]
    */
  def updateApplicationVersion(
    updateApplicationVersionRequest: UpdateApplicationVersionRequest
  ): Future[UpdateApplicationVersionResult] =
    wrapAsyncMethod(client.updateApplicationVersionAsync, updateApplicationVersionRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#updateEnvironment(com.amazonaws.services.elasticbeanstalk.model.UpdateEnvironmentRequest) AWS Java SDK]]
    */
  def updateEnvironment(
    updateEnvironmentRequest: UpdateEnvironmentRequest
  ): Future[UpdateEnvironmentResult] =
    wrapAsyncMethod(client.updateEnvironmentAsync, updateEnvironmentRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#validateConfigurationSettings(com.amazonaws.services.elasticbeanstalk.model.ValidateConfigurationSettingsRequest) AWS Java SDK]]
    */
  def validateConfigurationSettings(
    validateConfigurationSettingsRequest: ValidateConfigurationSettingsRequest
  ): Future[ValidateConfigurationSettingsResult] =
    wrapAsyncMethod(client.validateConfigurationSettingsAsync, validateConfigurationSettingsRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#shutdown() AWS Java SDK]]
    */
  def shutdown(): Unit =
    client.shutdown()

}
