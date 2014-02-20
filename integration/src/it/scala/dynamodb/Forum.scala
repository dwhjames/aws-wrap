package com.pellucid.wrap.dynamodb

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
