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

import S3.MFA
import models.{BatchDeletion, S3Object, Versions}

import java.io.File

import scala.concurrent.Future

import aws.core.Result
import aws.core.Types.EmptyResult


trait S3ObjectService {

    /**
      * Returns some or all (up to 1000) of the objects in a bucket.
      * To use this implementation of the operation, you must have READ access to the bucket.
      */
    def content(bucketname: String): Future[Result[S3Metadata, S3Object]]

    /**
      * List metadata about all of the versions of objects in a bucket
      */
    def getVersions(bucketname: String): Future[Result[S3Metadata, Versions]]

    /**
      * Adds an object to a bucket.
      * You must have WRITE permissions on a bucket to add an object to it.
      *
      * @param bucketname Name of the target bucket
      * @param body The File to upload to this Bucket
      */
    def put(bucketname: String, body: File): Future[EmptyResult[S3Metadata]]

    /**
      * Removes the null version (if there is one) of an object and inserts a delete marker,
      * which becomes the latest version of the object.
      * If there isn't a null version, Amazon S3 does not remove any objects.
      *
      * @param bucketname Name of the target bucket
      * @param objectName Name of the object to delete
      * @param versionId specific object version to delete
      * @param mfa Required to permanently delete a versioned object if versioning is configured with MFA Delete enabled.
      */
    def delete(
      bucketname: String,
      objectName: String,
      versionId:  Option[String] = None,
      mfa:        Option[MFA]    = None
    ): Future[EmptyResult[S3Metadata]]

    /**
      * Delete multiple objects from a bucket using a single HTTP request
      *
      * @param bucketname Name of the target bucket
      * @param objects Seq of objectName -> objectVersion
      * @param mfa Required to permanently delete a versioned object if versioning is configured with MFA Delete enabled.
      */
    def batchDelete(
      bucketname: String,
      objects:    Seq[(String, Option[String])],
      mfa:        Option[MFA]                    = None
    ): Future[Result[S3Metadata, BatchDeletion]]

}

trait S3ObjectServiceLayer {
  val s3ObjectService: S3ObjectService
}
