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

import aws.core.Result
import aws.core.Types.EmptyResult

import aws.s3.models.{CORSRule, S3Metadata}

trait CORSRuleService {

  /**
    * Sets the Cross-Origin Resource Sharing configuration for your bucket.
    * If the configuration exists, Amazon S3 replaces it.
    * To use this operation, you must be allowed to perform the
    * s3:PutCORSConfiguration action. By default, the bucket owner has this
    * permission and can grant it to others.
    * You can add up to 100 rules to the configuration
    *
    * @param bucketname The name of the bucket you want to set cors on.
    * @param rules List of CORSRule to apply on this Bucket
    */
  def create(bucketName: String, rules: CORSRule*): Future[EmptyResult[S3Metadata]]

  /**
    * Returns the cors configuration information set for the bucket.
    * To use this operation, you must have permission to perform the
    * s3:GetCORSConfiguration action. By default, the bucket owner has this
    * permission and can grant it to others.
    *
    * @param bucketname The name of the bucket.
    */
  def get(bucketName: String): Future[Result[S3Metadata, Seq[CORSRule]]]


  /**
    * Deletes the cors configuration information set for the bucket.
    * To use this operation, you must have permission to perform the s3:PutCORSConfiguration action. The bucket owner has this permission by default and can grant this permission to others.
    * @param bucketname The name of the bucket you want to delete.
    */
  def delete(bucketName: String): Future[EmptyResult[S3Metadata]]

}

trait CORSRuleServiceLayer {
  val corsRuleService: CORSRuleService
}
