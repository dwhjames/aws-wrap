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

package aws.s3

import aws.s3.modules._

/*

Example usage of this S3 library in a cake-pattern project

// anything that intends to use S3 should mixin this layer
trait S3ServiceLayer {
  val S3: AbstractS3Cake
}
// i.e., trait MyServiceLayer extends S3ServiceLayer { an abstract S3: AbstractS3Cake is now in scope }

// the project’s cake should mixin this concrete implementation of the layer
trait S3ServiceImplLayer
  extends S3ServiceLayer
     with ExecutionContextLayer
{

  override object S3 extends S3Cake {
    override lazy val s3HttpRequestExecutionContext = executionContext
  }
}
// i.e., object MyCake extends MyServiceLayer with S3ServiceImplLayer with ConcreteExecutionContextLayer

*/

trait AbstractS3Cake
  extends AbstractBucketLayer
     with AbstractCORSRuleLayer
     with AbstractLifecycleLayer
     with AbstractLoggingLayer
     with AbstractNotificationLayer
     with AbstractPolicyLayer
     with AbstractS3ObjectLayer
     with AbstractTagLayer

trait S3Cake extends AbstractS3Cake
  with BucketLayer
  with CORSRuleLayer
  with LifecycleLayer
  with LoggingLayer
  with NotificationLayer
  with PolicyLayer
  with S3ObjectLayer
  with TagLayer
  with HttpRequestLayer

object S3 extends S3Cake {

  /**
    * The current AWS key, read from the first line of `~/.awssecret`
    */
  override lazy val s3AwsKey: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(0)

  /**
    * The current AWS secret, read from the second line of `~/.awssecret`
    */
  override lazy val s3AwsSecret: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(1)

  /**
    * The execution context for executing the Http requests
    */
  override lazy val s3HttpRequestExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
}
