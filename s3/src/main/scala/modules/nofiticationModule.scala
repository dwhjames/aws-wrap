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

import S3Parsers._
import models.NotificationConfiguration

import scala.concurrent.Future
import scala.xml.Node

import aws.core.Result
import aws.core.Types.EmptyResult

trait NotificationModule {

    /**
      * Enable notifications of specified events for a bucket. Currently, the
      * s3:ReducedRedundancyLostObject event is the only event supported for notifications.
      * If the bucket owner and Amazon SNS topic owner are the same, the bucket owner
      * has permission to publish notifications to the topic by default.
      * Otherwise, the owner of the topic must create a policy to enable the bucket
      * owner to publish to the topic.
      * After you call {{{create}}} to configure notifications on a bucket, Amazon
      * S3 publishes a test notification to ensure that the topic exists and that the
      * bucket owner has permission to publish to the specified topic.
      *
      * @param bucketname The name of the bucket you want to enable notifications on
      * @param notification Notification configuration for this bucket
      */
    def create(bucketname: String, notification: NotificationConfiguration): Future[EmptyResult[S3Metadata]]

    /**
      * Disable notifications of specified events for a bucket.
      *
      * @param bucketname The name of the target bucket
      */
    def disable(bucketname: String): Future[EmptyResult[S3Metadata]]

    /**
      * return the notification configuration of a bucket.
      *
      * @param bucketname The name of the bucket you want to get notifications for
      */
    def get(bucketname: String): Future[Result[S3Metadata, Seq[NotificationConfiguration]]]

}

trait AbstractNotificationLayer {
  val Notification: NotificationModule
}

trait NotificationLayer extends AbstractNotificationLayer with AbstractHttpRequestLayer {

  override object Notification extends NotificationModule {

    def create(bucketname: String, notification: NotificationConfiguration): Future[EmptyResult[S3Metadata]] = {
      val b =
        <NotificationConfiguration>
           <TopicConfiguration>
               <Topic>{ notification.topic }</Topic>
               <Event>{ notification.event.toString }</Event>
           </TopicConfiguration>
        </NotificationConfiguration>

      Http.put[Node, Unit](
        Some(bucketname),
        body = b,
        subresource = Some("notification")
      )
    }

    def disable(bucketname: String): Future[EmptyResult[S3Metadata]] = {
      val b = <NotificationConfiguration />
      Http.put[Node, Unit](
        Some(bucketname),
        body = b,
        subresource = Some("notification")
      )
    }

    def get(bucketname: String): Future[Result[S3Metadata, Seq[NotificationConfiguration]]] =
      Http.get[Seq[NotificationConfiguration]](
        Some(bucketname),
        subresource = Some("notification")
      )

  }

}
