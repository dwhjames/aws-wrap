package com.pellucid.wrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat


case class Reply(
  id:            String,
  replyDateTime: DateTime,
  message:       String,
  postedBy:      String
)

object Reply {

  val tableName = "Reply"
  val secondaryIndexName = "PostedByIndex"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(Reply.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(
      Schema.stringAttribute(Attributes.id),
      Schema.stringAttribute(Attributes.replyDateTime),
      Schema.stringAttribute(Attributes.postedBy)
    )
    .withKeySchema(
      Schema.hashKey(Attributes.id),
      Schema.rangeKey(Attributes.replyDateTime)
    )
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
      .withIndexName(Reply.secondaryIndexName)
      .withKeySchema(
        Schema.hashKey(Attributes.id),
        Schema.rangeKey(Attributes.postedBy)
      )
      .withProjection(
        new Projection()
        .withProjectionType(ProjectionType.KEYS_ONLY)
      )
    )

  object Attributes {
    val id            = "Id"
    val replyDateTime = "ReplyDateTime"
    val message       = "Message"
    val postedBy      = "PostedBy"
  }

  implicit object replySerializer extends DynamoDBSerializer[Reply] {
    private val fmt = ISODateTimeFormat.dateTime

    override val tableName = Reply.tableName
    override val hashAttributeName = Attributes.id
    override val rangeAttributeName = Some(Attributes.replyDateTime)

    override def primaryKeyOf(reply: Reply) =
      Map(
        Attributes.id            -> reply.id,
        Attributes.replyDateTime -> fmt.print(reply.replyDateTime)
      )

    override def toAttributeMap(reply: Reply) =
      Map(
        Attributes.id            -> reply.id,
        Attributes.replyDateTime -> fmt.print(reply.replyDateTime),
        Attributes.message       -> reply.message,
        Attributes.postedBy      -> reply.postedBy
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      Reply(
        id            = item(Attributes.id),
        replyDateTime = fmt.parseDateTime(item(Attributes.replyDateTime)),
        message       = item(Attributes.message),
        postedBy      = item(Attributes.postedBy)
      )
  }
}
