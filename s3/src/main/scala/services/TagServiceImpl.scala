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

import S3Parsers._
import models.Tag

import scala.concurrent.Future
import scala.xml.Node

import aws.core.Result
import aws.core.Types.EmptyResult


trait TagServiceImplLayer
  extends TagServiceLayer
     with HttpRequestLayer
{

  override object tagService extends TagService {

    /**
      * Removes a tag set from the specified bucket.
      *
      * @param bucketname Name of the tagged bucket
      */
    def delete(bucketname: String): Future[EmptyResult[S3Metadata]] =
      Http.delete[Unit](
        Some(bucketname),
        subresource = Some("tagging")
      )

    /**
      * Returns the tag set associated with the bucket.
      *
      * @param bucketname Name of the tagged bucket
      */
    def get(bucketname: String): Future[Result[S3Metadata, Seq[Tag]]] =
      Http.get[Seq[Tag]](
        Some(bucketname),
        subresource = Some("tagging")
      )

    /**
      * Adds a set of tags to an existing bucket.
      *
      * @param bucketname Name of the bucket to tag
      * @param tags Tags to set on this Bucket
      */
    def create(bucketname: String, tags: Tag*): Future[EmptyResult[S3Metadata]] = {
      val b =
        <Tagging>
          <TagSet>
            {
              for (t <- tags) yield
              <Tag>
                <Key>{ t.name }</Key>
                <Value>{ t.value }</Value>
              </Tag>
            }
          </TagSet>
        </Tagging>

      val ps = Seq(Parameters.MD5(b.mkString))
      Http.put[Node, Unit](
        Some(bucketname),
        body = b,
        subresource = Some("tagging"),
        parameters = ps
      )
    }

  }

}
