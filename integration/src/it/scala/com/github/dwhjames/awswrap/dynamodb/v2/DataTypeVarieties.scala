package com.github.dwhjames.awswrap.dynamodb.v2

import DataTypeConversions._
import com.amazonaws.services.dynamodbv2.model._
import com.github.dwhjames.awswrap.dynamodb.{Schema, DynamoDBSerializer} //don't import '_' as legacy DataTypeConversions will clash with new ones

case class DataTypeVarieties(
    string:           String,
    bigDecimal:       BigDecimal,
    bigInt:           BigInt,
    double:           Double,
    float:            Float,
    long:             Long,
    int:              Int,
    byte:             Byte,
    short:            Short,
    bool:             Boolean,
    strings:          Set[String],
    ints:             Set[Int],
    bytes:            Set[Byte],
    listOfStrings:    List[String],
    mapOfInts:        Map[String, Int]
)

object DataTypeVarieties {

  val tableName = "DataTypeVarieties"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(DataTypeString.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(
      Schema.stringAttribute(Attributes.string)
    )
    .withKeySchema(
      Schema.hashKey(Attributes.string)
    )

  object Attributes {
    val string         = "string"
    val bigDecimal     = "bigDecimal"
    val bigInt         = "bigInt"
    val double         = "double"
    val float          = "float"
    val long           = "long"
    val int            = "int"
    val byte           = "byte"
    val short          = "short"
    val bool           = "boolean"
    val strings        = "strings"
    val ints           = "ints"
    val bytes          = "bytes"
    val listOfStrings  = "listOfStrings"
    val mapOfInts      = "map"
  }

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeVarieties] {

    override val tableName = DataTypeVarieties.tableName
    override val hashAttributeName = Attributes.string

    override def primaryKeyOf(dtypes: DataTypeVarieties) =
      Map(
        Attributes.string -> dtypes.string
      )


    override def toAttributeMap(dtypes: DataTypeVarieties) =
      Map(
        Attributes.string -> dtypes.string,
        Attributes.bigDecimal -> dtypes.bigDecimal,
        Attributes.bigInt -> dtypes.bigInt,
        Attributes.double -> dtypes.double,
        Attributes.float -> dtypes.float,
        Attributes.long -> dtypes.long,
        Attributes.int -> dtypes.int,
        Attributes.byte -> dtypes.byte,
        Attributes.short -> dtypes.short,
        Attributes.bool -> dtypes.bool,
        Attributes.strings -> dtypes.strings,
        Attributes.ints -> dtypes.ints,
        Attributes.bytes -> dtypes.bytes,
        Attributes.listOfStrings -> dtypes.listOfStrings,
        Attributes.mapOfInts -> dtypes.mapOfInts
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeVarieties(
        string = item(Attributes.string),
        bigDecimal = item(Attributes.bigDecimal),
        bigInt = item(Attributes.bigInt),
        double = item(Attributes.double),
        float = item(Attributes.float),
        long = item(Attributes.long),
        int = item(Attributes.int),
        byte = item(Attributes.byte),
        short = item(Attributes.short),
        bool = attributeValueToBoolean(item(Attributes.bool)),
        strings = item(Attributes.strings),
        ints = item(Attributes.ints),
        bytes = item(Attributes.bytes),
        listOfStrings = item(Attributes.listOfStrings),
        mapOfInts = item(Attributes.mapOfInts)
      )
  }
}
