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
package modules

import models.Tag

import scala.concurrent.Future
import scala.xml.Node

trait TagModule {

    /**
      * Removes a tag set from the specified bucket.
      *
      * @param bucketname Name of the tagged bucket
      */
    def delete(bucketname: String): Future[EmptyS3Result]

    /**
      * Returns the tag set associated with the bucket.
      *
      * @param bucketname Name of the tagged bucket
      */
    def get(bucketname: String): Future[S3Result[Seq[Tag]]]

    /**
      * Adds a set of tags to an existing bucket.
      *
      * @param bucketname Name of the bucket to tag
      * @param tags Tags to set on this Bucket
      */
    def create(bucketname: String, tags: Tag*): Future[EmptyS3Result]

}

trait AbstractTagLayer {
  val Tag: TagModule
}

trait TagLayer extends AbstractTagLayer with AbstractHttpRequestLayer {

  override object Tag extends TagModule {

    def delete(bucketname: String): Future[EmptyS3Result] =
      Http.delete[Unit](
        Some(bucketname),
        subresource = Some("tagging")
      )

    def get(bucketname: String): Future[S3Result[Seq[Tag]]] =
      Http.get[Seq[Tag]](
        Some(bucketname),
        subresource = Some("tagging")
      )

    def create(bucketname: String, tags: Tag*): Future[EmptyS3Result] = {
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
