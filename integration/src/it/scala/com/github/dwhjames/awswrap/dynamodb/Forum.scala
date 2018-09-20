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


case class Forum(
  name:     String,
  category: String,
  threads:  Long,
  messages: Long,
  views:    Long
)


object Forum {

  val tableName = "Forum"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(Forum.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(Schema.stringAttribute(Attributes.name))
    .withKeySchema(Schema.hashKey(Attributes.name))

  object Attributes {
    val name     = "Name"
    val category = "Category"
    val threads  = "Threads"
    val messages = "Messages"
    val views    = "Views"
  }

  implicit object forumSerializer extends DynamoDBSerializer[Forum] {
    override val tableName = Forum.tableName
    override val hashAttributeName = Attributes.name

    override def primaryKeyOf(forum: Forum) =
      Map(Attributes.name -> forum.name)

    override def toAttributeMap(forum: Forum) =
      Map(
        Attributes.name     -> forum.name,
        Attributes.category -> forum.category,
        Attributes.threads  -> forum.threads,
        Attributes.messages -> forum.messages,
        Attributes.views    -> forum.views
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      Forum(
        name     = item(Attributes.name),
        category = item(Attributes.category),
        threads  = item(Attributes.threads),
        messages = item(Attributes.messages),
        views    = item(Attributes.views)
      )
  }
}
