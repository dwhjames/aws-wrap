/*
 * Copyright 2012-2015 Pellucid Analytics
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
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticbeanstalk/AWSElasticBeanstalk.html#shutdown() AWS Java SDK]]
    */
  def shutdown(): Unit =
    client.shutdown()

}
