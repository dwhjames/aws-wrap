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

import scala.concurrent.Future

import aws.core.Result
import aws.core.Types.EmptyResult

import aws.s3.S3Metadata
import aws.s3.models.Tag

trait TagService {

    /**
      * Removes a tag set from the specified bucket.
      *
      * @param bucketname Name of the tagged bucket
      */
    def delete(bucketname: String): Future[EmptyResult[S3Metadata]]

    /**
      * Returns the tag set associated with the bucket.
      *
      * @param bucketname Name of the tagged bucket
      */
    def get(bucketname: String): Future[Result[S3Metadata, Seq[Tag]]]

    /**
      * Adds a set of tags to an existing bucket.
      *
      * @param bucketname Name of the bucket to tag
      * @param tags Tags to set on this Bucket
      */
    def create(bucketname: String, tags: Tag*): Future[EmptyResult[S3Metadata]]

}

trait TagServiceLayer {
  val tagService: TagService
}
