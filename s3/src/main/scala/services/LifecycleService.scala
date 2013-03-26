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

package aws.s3.services

import scala.concurrent.Future
import scala.xml.Node

import aws.core.Result
import aws.core.Types.EmptyResult

import aws.s3.S3Parsers._
import aws.s3.models.{LifecycleConf, S3Metadata}

trait LifecycleService {

  /**
    * Sets lifecycle configuration for your bucket. It lifecycle configuration exists, it replaces it.
    * To use this operation, you must be allowed to perform the s3:PutLifecycleConfiguration action. By default, the bucket owner has this permission and can grant this permission to others.
    * There is usually some lag before a new or updated lifecycle configuration is fully propagated to all the Amazon S3 systems. You should expect some delay before lifecycle configuration fully taking effect.
    * @param bucketname The name of the bucket you want to set LifecycleConf on.
    * @param confs List of LifecycleConf to apply
    */
  def create(bucketname: String, confs: LifecycleConf*): Future[EmptyResult[S3Metadata]]

  /**
    * Returns the lifecycle configuration information set on the bucket.
    * To use this operation, you must have permission to perform the s3:GetLifecycleConfiguration action
    * @param bucketname The name of the bucket.
    */
  def get(bucketname: String): Future[Result[S3Metadata, Seq[LifecycleConf]]]

  /**
    * Deletes the lifecycle configuration from the specified bucket.
    * @param bucketname The name of the bucket.
    */
  def delete(bucketname: String): Future[EmptyResult[S3Metadata]]

}

trait LifecycleServiceLayer {
  val lifecycleService: LifecycleService
}
