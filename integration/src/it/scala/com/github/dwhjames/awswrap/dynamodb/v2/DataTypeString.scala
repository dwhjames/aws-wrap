package com.github.dwhjames.awswrap.dynamodb.v2

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, CreateTableRequest}
import com.github.dwhjames.awswrap.dynamodb.v2.DataTypeConversions._
import com.github.dwhjames.awswrap.dynamodb.{DynamoDBSerializer, Schema} //don't import '_' as legacy DataTypeConversions will clash with new ones

case class DataTypeString(
    key:     String,
    set:     Set[String],
    list:    List[String],
    map:     Map[String, String]
)

object DataTypeString {

  val tableName = "DataTypeString"

  val tableRequest =
    new CreateTableRequest()
      .withTableName(DataTypeString.tableName)
      .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
      .withAttributeDefinitions(
        Schema.stringAttribute(Attributes.key)
      )
      .withKeySchema(
        Schema.hashKey(Attributes.key)
      )

  object Attributes {
    val key   = "key"
    val set   = "set"
    val list  = "list"
    val map   = "map"
  }

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeString] {

    override val tableName = DataTypeString.tableName
    override val hashAttributeName = Attributes.key

    override def primaryKeyOf(dtypes: DataTypeString) =
      Map(
        Attributes.key -> dtypes.key
      )

    override def toAttributeMap(dtypes: DataTypeString) =
      Map(
        Attributes.key -> dtypes.key,
        Attributes.set -> dtypes.set,
        Attributes.list -> dtypes.list,
        Attributes.map -> dtypes.map
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeString(
        key = item(Attributes.key),
        set = item(Attributes.set),
        list = item(Attributes.list),
        map = item(Attributes.map)
      )
  }
}
