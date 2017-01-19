package com.github.dwhjames.awswrap.dynamodb.v2

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, CreateTableRequest}
import com.github.dwhjames.awswrap.dynamodb.v2.DataTypeConversions._
import com.github.dwhjames.awswrap.dynamodb.{DynamoDBSerializer, Schema} //don't import '_' as legacy DataTypeConversions will clash with new ones

case class DataTypeByte(
    key:     Byte,
    set:     Set[Byte],
    list:    List[Byte],
    map:     Map[String, Byte],
    array:   Array[Byte],
    setOfArrays: Iterable[Array[Byte]]
)

object DataTypeByte {

  val tableName = "DataTypeByte"

  val tableRequest =
    new CreateTableRequest()
      .withTableName(DataTypeByte.tableName)
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
    val array   = "array"
    val setOfArrays   = "setOfArrays"
  }

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeByte] {

    override val tableName = DataTypeByte.tableName
    override val hashAttributeName = Attributes.key

    override def primaryKeyOf(dtypes: DataTypeByte) =
      Map(
        Attributes.key -> dtypes.key
      )

    override def toAttributeMap(dtypes: DataTypeByte) =
      Map(
        Attributes.key -> dtypes.key,
        Attributes.set -> dtypes.set,
        Attributes.list -> dtypes.list,
        Attributes.map -> dtypes.map,
        Attributes.array -> dtypes.array,
        Attributes.setOfArrays -> dtypes.setOfArrays
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeByte(
        key  = item(Attributes.key),
        set  = item(Attributes.set),
        list = item(Attributes.list),
        map  = item(Attributes.map),
        array  = item(Attributes.array),
        setOfArrays  = item(Attributes.setOfArrays)
      )
  }
}
