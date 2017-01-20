package com.github.dwhjames.awswrap.dynamodb.v2

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, CreateTableRequest}
import com.github.dwhjames.awswrap.dynamodb.v2.DataTypeConversions._
import com.github.dwhjames.awswrap.dynamodb.{DynamoDBSerializer, Schema} //don't import '_' as legacy DataTypeConversions will clash with new ones

case class DataTypeBoolean(
    key:     String,
    value:   Boolean,
    set:     Set[Boolean],
    list:    List[Boolean],
    map:     Map[String, Boolean]
)

object DataTypeBoolean {

  val tableName = "DataTypeBoolean"

  val tableRequest =
    new CreateTableRequest()
      .withTableName(DataTypeBoolean.tableName)
      .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
      .withAttributeDefinitions(
        Schema.stringAttribute(Attributes.key)
      )
      .withKeySchema(
        Schema.hashKey(Attributes.key)
      )

  object Attributes {
    val key   = "key"
    val value = "value"
    val set   = "set"
    val list  = "list"
    val map   = "map"
  }

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeBoolean] {

    override val tableName = DataTypeBoolean.tableName
    override val hashAttributeName = Attributes.key

    override def primaryKeyOf(dtypes: DataTypeBoolean) =
      Map(
        Attributes.key -> dtypes.key
      )

    override def toAttributeMap(dtypes: DataTypeBoolean) =
      Map(
        Attributes.key -> dtypes.key,
        Attributes.value -> dtypes.value,
        Attributes.set -> dtypes.set,
        Attributes.list -> dtypes.list,
        Attributes.map -> dtypes.map
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeBoolean(
        key   = item(Attributes.key),
        value = item(Attributes.value),
        set   = item(Attributes.set),
        list  = item(Attributes.list),
        map   = item(Attributes.map)
      )
  }
}
