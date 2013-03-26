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
package services

import models.Policy

import scala.concurrent.Future

import aws.core.Result
import aws.core.Types.EmptyResult


trait PolicyService {

  /**
    * Add to or replace a policy on a bucket. If the bucket already has a policy,
    * the one in this request completely replaces it.
    * To perform this operation, you must be the bucket owner or have PutPolicy permissions.
    *
    * @param bucketname The name of the bucket you want to enable Policy on
    * @param policy policy configuration for this bucket
    *
    * {{{
    * val policy = Policy(
    *   id = Some("aaaa-bbbb-cccc-dddd"),
    *   statements = Seq(
    *     Statement(
    *       effect = Policy.Effects.ALLOW,
    *       sid = Some("1"),
    *       principal = Some("AWS" -> Seq("*")),
    *       action = Seq("s3:GetObject*"),
    *       conditions = Seq(
    *         Conditions.Strings.Equals(USER_AGENT -> Seq("foo")),
    *         Conditions.Exists(KeyFor(REFERER) -> Seq(true))),
    *       resource = Seq("arn:aws:s3:::bucketName/foobar")))) //  Make sure the bucketname is in lower case
    *
    * Policy.create(bucketName, policy)
    * }}}
    *
    */
  def create(bucketname: String, policy: Policy): Future[EmptyResult[S3Metadata]]

  /**
    * Return the policy of a specified bucket. To use this operation,
    * you must have GetPolicy permissions on the specified bucket, and you
    * must be the bucket owner.
    *
    * @param bucketname The name of the bucket you want to get Policy on
    */
  def get(bucketname: String): Future[Result[S3Metadata, Policy]]

  /**
    * delete the policy on a specified bucket. To use the operation,
    * you must have DeletePolicy permissions on the specified bucket
    * and be the bucket owner.
    */
  def delete(bucketname: String): Future[EmptyResult[S3Metadata]]

}

trait PolicyServiceLayer {
  val policyService: PolicyService
}
