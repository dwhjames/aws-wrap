
package aws.wrap

import scala.collection.JavaConverters._

import com.amazonaws.services.dynamodbv2.model._

package object dynamodb {

  type AttributeValue = com.amazonaws.services.dynamodbv2.model.AttributeValue

  /*
   * Query construction helpers
   */
  def mkHashKeyQuery[K <% AttributeValue](hashAttr: (String, K)): QueryRequest =
    new QueryRequest()
    .withKeyConditions(
      Map(
        hashAttr._1 -> QueryCondition.equalTo(hashAttr._2)
      ).asJava
    )

  def mkHashKeyQuery[T, K <% AttributeValue]
                    (hashValue: K)
                    (implicit serializer: DynamoDBSerializer[T])
                    : QueryRequest =
    mkHashKeyQuery(serializer.hashAttributeName -> hashValue)

  def mkHashAndRangeKeyQuery[K <% AttributeValue]
                            (hashAttr: (String, K), rangeAttr: (String, Condition))
                            : QueryRequest =
    new QueryRequest()
    .withKeyConditions(
      Map(
        hashAttr._1  -> QueryCondition.equalTo(hashAttr._2),
        rangeAttr._1 -> rangeAttr._2
      ).asJava
    )

  def mkHashAndRangeKeyQuery[T, K <% AttributeValue]
                            (hashValue: K, rangeCondition: Condition)
                            (implicit serializer: DynamoDBSerializer[T])
                            : QueryRequest =
    mkHashAndRangeKeyQuery(
      serializer.hashAttributeName -> hashValue,
      serializer.rangeAttributeName.getOrElse {
        throw new UnsupportedOperationException(s"mkHashAndRangeKeyQuery: table ${serializer.tableName} does not have a range key")
      } -> rangeCondition
    )

  /*
   * Attribute value helpers
   */
  // string attribute value
  implicit val stringToAttributeValue = (x: String) => new AttributeValue().withS(x)

  implicit val stringIterableToAttributeValue = (x: Iterable[String]) => new AttributeValue().withSS(x.asJavaCollection)


  // numeric attribute value
  implicit val doubleToAttributeValue = (x: Double) => new AttributeValue().withN(x.toString)
  implicit val floatToAttributeValue  = (x: Float)  => new AttributeValue().withN(x.toString)
  implicit val longToAttributeValue   = (x: Long)   => new AttributeValue().withN(x.toString)
  implicit val intToAttributeValue    = (x: Int)    => new AttributeValue().withN(x.toString)
  implicit val shortToAttributeValue  = (x: Short)  => new AttributeValue().withN(x.toString)
  implicit val byteToAttributeValue   = (x: Byte)   => new AttributeValue().withN(x.toString)

  implicit val doubleIterableToAttributeValue = (x: Iterable[Double]) => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  implicit val floatIterableToAttributeValue  = (x: Iterable[Float])  => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  implicit val longIterableToAttributeValue   = (x: Iterable[Long])   => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  implicit val intIterableToAttributeValue    = (x: Iterable[Int])    => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  implicit val shortIterableToAttributeValue  = (x: Iterable[Short])  => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)
  implicit val byteIterableToAttributeValue   = (x: Iterable[Byte])   => new AttributeValue().withNS(x.map(_.toString).asJavaCollection)


  // binary attribute value
  implicit val byteArrayToAttributeValue = (x: Array[Byte]) => new AttributeValue().withB(java.nio.ByteBuffer.wrap(x))

  implicit val byteArrayIterableToAttributeValue = (x: Iterable[Array[Byte]]) => new AttributeValue().withBS(x.map(java.nio.ByteBuffer.wrap(_)).asJavaCollection)


  // extras
  implicit val booleanToAttributeValue = (x: Boolean) => new AttributeValue().withS(x.toString)

  implicit val booleanIterableToAttributeValue = (x: Iterable[Boolean]) => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)


  implicit val bigIntToAttributeValue     = (x: BigInt)     => new AttributeValue().withS(x.toString)
  implicit val bigDecimalToAttributeValue = (x: BigDecimal) => new AttributeValue().withS(x.toString)

  implicit val bigIntIterableToAttributeValue     = (x: Iterable[BigInt])     => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)
  implicit val bigDecimalIterableToAttributeValue = (x: Iterable[BigDecimal]) => new AttributeValue().withSS(x.map(_.toString).asJavaCollection)

  /*
   * Enrich AttributeValue
   */
  implicit class RichAttributeValue(attrVal: AttributeValue) {
    def as[T](implicit conv: AttributeValue => T): T =
      catchAndRethrowConversion(conv(attrVal))
  }

  private def catchAndRethrowConversion[T](x: => T): T =
    try {
      x
    } catch {
      case ex: NullPointerException =>
          throw new AttributeValueConversionException("RichAttributeValue.as: AttributeValue was not of the expected sort.")
      case ex: NumberFormatException =>
        throw new AttributeValueConversionException("RichAttributeValue.as: the numeric string can't be parsed as the expected numeric type.")
    }

  implicit val attributeValueToString = (x: AttributeValue) => catchAndRethrowConversion { x.getS }

  implicit val attributeValueToStringSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.toSet }


  implicit val attributeValueToDouble = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toDouble }
  implicit val attributeValueToFloat  = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toFloat }
  implicit val attributeValueToLong   = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toLong }
  implicit val attributeValueToInt    = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toInt }
  implicit val attributeValueToShort  = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toShort }
  implicit val attributeValueToByte   = (x: AttributeValue) => catchAndRethrowConversion { x.getN.toByte }

  implicit val attributeValueToDoubleSet = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toDouble).toSet }
  implicit val attributeValueToFloatSet  = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toFloat).toSet }
  implicit val attributeValueToLongSet   = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toLong).toSet }
  implicit val attributeValueToIntSet    = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toInt).toSet }
  implicit val attributeValueToShortSet  = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toShort).toSet }
  implicit val attributeValueToByteSet   = (x: AttributeValue) => catchAndRethrowConversion { x.getNS.asScala.map(_.toByte).toSet }


  implicit val attributeValueToByteArray = (x: AttributeValue) => catchAndRethrowConversion { x.getB.array }

  implicit val attributeValueToByteArraySet = (x: AttributeValue) => catchAndRethrowConversion { x.getBS.asScala.map(_.array).toSet }


  implicit val attributeValueToBoolean = (x: AttributeValue) => catchAndRethrowConversion { x.getS.toBoolean }

  implicit val attributeValueToBooleanSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(_.toBoolean).toSet }


  implicit val attributeValueToBigInt     = (x: AttributeValue) => catchAndRethrowConversion { BigInt(x.getS) }
  implicit val attributeValueToBigDecimal = (x: AttributeValue) => catchAndRethrowConversion { BigDecimal(x.getS) }

  implicit val attributeValueToBigIntSet     = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(BigInt(_)).toSet }
  implicit val attributeValueToBigDecimalSet = (x: AttributeValue) => catchAndRethrowConversion { x.getSS.asScala.map(BigDecimal(_)).toSet }

}
