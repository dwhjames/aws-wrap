package com.github.dwhjames.awswrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._

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
    bytes:            Set[Byte]
)

object DataTypeVarieties {

  val tableName = "DataTypeVarieties"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(DataTypeVarieties.tableName)
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
  }

  implicit object sameScoreSerializer extends DynamoDBSerializer[DataTypeVarieties] {

    override val tableName = DataTypeVarieties.tableName
    override val hashAttributeName = Attributes.string

    override def primaryKeyOf(dtypes: DataTypeVarieties) =
      Map(
        Attributes.string -> dtypes.string
      )

    def convertSS(f: Set[String])(implicit p: Set[String] => AttributeValue) = p(f)
    def convertSI(f: Set[Int])(implicit p: Set[Int] => AttributeValue) = p(f)
    def convertSB(f: Set[Byte])(implicit p: Set[Byte] => AttributeValue) = p(f)

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
        Attributes.strings -> convertSS(dtypes.strings),
        Attributes.ints -> convertSI(dtypes.ints),
        Attributes.bytes -> convertSB(dtypes.bytes)
      )

    def convertD(x: AttributeValue)(implicit p: AttributeValue => Double ) = p(x)
    def convertF(x: AttributeValue)(implicit p: AttributeValue => Float ) = p(x)
    def convertL(x: AttributeValue)(implicit p: AttributeValue => Long ) = p(x)
    def convertI(x: AttributeValue)(implicit p: AttributeValue => Int ) = p(x)
    def convertB(x: AttributeValue)(implicit p: AttributeValue => Byte ) = p(x)
    def convertS(x: AttributeValue)(implicit p: AttributeValue => Short ) = p(x)

    def convertASS(x: AttributeValue)(implicit p: AttributeValue => Set[String] ):Set[String] = p(x)
    def convertASI(x: AttributeValue)(implicit p: AttributeValue => Set[Int] ):Set[Int] = p(x)
    def convertASB(x: AttributeValue)(implicit p: AttributeValue => Set[Byte] ):Set[Byte] = p(x)

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      DataTypeVarieties(
        string = item(Attributes.string),
        bigDecimal = item(Attributes.bigDecimal),
        bigInt = item(Attributes.bigInt),
        double = convertD(item(Attributes.double)),
        float = convertF(item(Attributes.float)),
        long = convertL(item(Attributes.long)),
        int = convertI(item(Attributes.int)),
        byte = convertB(item(Attributes.byte)),
        short = convertS(item(Attributes.short)),
        bool = attributeValueToBoolean(item(Attributes.bool)),
        strings = convertASS(item(Attributes.strings)),
        ints = convertASI(item(Attributes.ints)),
        bytes = convertASB(item(Attributes.bytes))
      )
  }
}
