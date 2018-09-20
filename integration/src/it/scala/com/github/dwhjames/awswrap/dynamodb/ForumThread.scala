/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
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

package com.github.dwhjames.awswrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat


case class ForumThread(
  forumName:          String,
  subject:            String,
  message:            String,
  lastPostedBy:       String,
  lastPostedDateTime: DateTime,
  views:              Long,
  replies:            Long,
  answered:           Long,
  tags:               Set[String]
)

object ForumThread {

  val tableName = "Thread"
  val secondaryIndexName = "LastPostedIndex"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(ForumThread.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(
      Schema.stringAttribute(Attributes.forumName),
      Schema.stringAttribute(Attributes.subject),
      Schema.stringAttribute(Attributes.lastPostedDateTime)
    )
    .withKeySchema(
      Schema.hashKey(Attributes.forumName),
      Schema.rangeKey(Attributes.subject)
    )
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
      .withIndexName(ForumThread.secondaryIndexName)
      .withKeySchema(
        Schema.hashKey(Attributes.forumName),
        Schema.rangeKey(Attributes.lastPostedDateTime)
      )
      .withProjection(
        new Projection()
        .withProjectionType(ProjectionType.KEYS_ONLY)
      )
    )

  object Attributes {
    val forumName          = "ForumName"
    val subject            = "Subject"
    val message            = "Message"
    val lastPostedBy       = "LastPostedBy"
    val lastPostedDateTime = "LastPostedDateTime"
    val views              = "Views"
    val replies            = "Replies"
    val answered           = "Answered"
    val tags               = "Tags"
  }

  implicit object forumThreadSerializer extends DynamoDBSerializer[ForumThread] {
    private val fmt = ISODateTimeFormat.dateTime

    override val tableName = ForumThread.tableName
    override val hashAttributeName = Attributes.forumName
    override val rangeAttributeName = Some(Attributes.subject)

    override def primaryKeyOf(thread: ForumThread) =
      Map(
        Attributes.forumName -> thread.forumName,
        Attributes.subject   -> thread.subject
      )

    override def toAttributeMap(thread: ForumThread) =
      Map(
        Attributes.forumName          -> thread.forumName,
        Attributes.subject            -> thread.subject,
        Attributes.message            -> thread.message,
        Attributes.lastPostedBy       -> thread.lastPostedBy,
        Attributes.lastPostedDateTime -> fmt.print(thread.lastPostedDateTime),
        Attributes.views              -> thread.views,
        Attributes.replies            -> thread.replies,
        Attributes.answered           -> thread.answered,
        Attributes.tags               -> thread.tags
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      ForumThread(
        forumName          = item(Attributes.forumName),
        subject            = item(Attributes.subject),
        message            = item(Attributes.message),
        lastPostedBy       = item(Attributes.lastPostedBy),
        lastPostedDateTime = fmt.parseDateTime(item(Attributes.lastPostedDateTime)),
        views              = item(Attributes.views),
        replies            = item(Attributes.replies),
        answered           = item(Attributes.answered),
        tags               = item(Attributes.tags)
      )
  }
}
