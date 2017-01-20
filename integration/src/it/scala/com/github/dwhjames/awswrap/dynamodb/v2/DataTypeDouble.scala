package com.github.dwhjames.awswrap.dynamodb.v2

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, CreateTableRequest}
import com.github.dwhjames.awswrap.dynamodb.v2.DataTypeConversions._
import com.github.dwhjames.awswrap.dynamodb.{DynamoDBSerializer, Schema} //don't import '_' as legacy DataTypeConversions will clash with new ones

case class DataTypeDouble(
    key:     Double,
    set:     Set[Double],
    list:    List[Double],
    map:     Map[String, Double]
)

object DataTypeDouble {

  val tableName = "DataTypeDouble"

  val tableRequest =
    new CreateTableRequest()
      .withTableName(DataTypeDouble.tableName)
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

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeDouble] {

    override val tableName = DataTypeDouble.tableName
    override val hashAttributeName = Attributes.key

    override def primaryKeyOf(dtypes: DataTypeDouble) =
      Map(
        Attributes.key -> dtypes.key
      )

    override def toAttributeMap(dtypes: DataTypeDouble) =
      Map(
        Attributes.key -> dtypes.key,
        Attributes.set -> dtypes.set,
        Attributes.list -> dtypes.list,
        Attributes.map -> dtypes.map
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeDouble(
        key  = item(Attributes.key),
        set  = item(Attributes.set),
        list = item(Attributes.list),
        map  = item(Attributes.map)
      )
  }
}
