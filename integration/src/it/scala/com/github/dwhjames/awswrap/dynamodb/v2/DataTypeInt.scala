package com.github.dwhjames.awswrap.dynamodb.v2

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, CreateTableRequest}
import com.github.dwhjames.awswrap.dynamodb.v2.DataTypeConversions._
import com.github.dwhjames.awswrap.dynamodb.{DynamoDBSerializer, Schema} //don't import '_' as legacy DataTypeConversions will clash with new ones

case class DataTypeInt(
    key:     Int,
    set:     Set[Int],
    list:    List[Int],
    map:     Map[String, Int]
)

object DataTypeInt {

  val tableName = "DataTypeInt"

  val tableRequest =
    new CreateTableRequest()
      .withTableName(DataTypeInt.tableName)
      .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
      .withAttributeDefinitions(
        Schema.numberAttribute(Attributes.key)
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

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeInt] {

    override val tableName = DataTypeInt.tableName
    override val hashAttributeName = Attributes.key

    override def primaryKeyOf(dtypes: DataTypeInt) =
      Map(
        Attributes.key -> dtypes.key
      )

    override def toAttributeMap(dtypes: DataTypeInt) =
      Map(
        Attributes.key -> dtypes.key,
        Attributes.set -> dtypes.set,
        Attributes.list -> dtypes.list,
        Attributes.map -> dtypes.map
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeInt(
        key  = item(Attributes.key),
        set  = item(Attributes.set),
        list = item(Attributes.list),
        map  = attributeValueToMapOfInts(item(Attributes.map)) //TODO: why implicit conversion not working???
      )
  }
}
