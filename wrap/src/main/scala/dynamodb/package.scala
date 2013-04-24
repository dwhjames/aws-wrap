
package aws.wrap

import scala.collection.JavaConverters._

import com.amazonaws.services.dynamodbv2.model._

package object dynamodb {

  /*
   * Query construction helpers
   */
  def mkHashKeyQuery[K <% AttributeValue](hashAttr: (String, K)): QueryRequest =
    new QueryRequest()
    .withKeyConditions(
      Map(
        hashAttr._1 -> QueryCondition.equals(hashAttr._2)
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
        hashAttr._1  -> QueryCondition.equals(hashAttr._2),
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

  // numeric attribute value
  implicit val doubleToAttributeValue = (x: Double) => new AttributeValue().withN(x.toString)
  implicit val floatToAttributeValue  = (x: Float)  => new AttributeValue().withN(x.toString)
  implicit val longToAttributeValue   = (x: Long)   => new AttributeValue().withN(x.toString)
  implicit val intToAttributeValue    = (x: Int)    => new AttributeValue().withN(x.toString)
  implicit val shortToAttributeValue  = (x: Short)  => new AttributeValue().withN(x.toString)
  implicit val byteToAttributeValue   = (x: Byte)   => new AttributeValue().withN(x.toString)

  // binary attribute value
  implicit val byteArrayToAttributeValue = (x: Array[Byte]) => new AttributeValue().withB(java.nio.ByteBuffer.wrap(x))

  /*
   * Enrich AttributeValue
   */
  implicit class RichAttributeValue(attrVal: AttributeValue) {
    def as[T](implicit conv: AttributeValue => T): T = conv(attrVal)
  }

  implicit val attributeValueToString = (x: AttributeValue) => x.getS

  implicit val attributeValueToDouble = (x: AttributeValue) => x.getN.toDouble
  implicit val attributeValueToFloat  = (x: AttributeValue) => x.getN.toFloat
  implicit val attributeValueToLong   = (x: AttributeValue) => x.getN.toLong
  implicit val attributeValueToInt    = (x: AttributeValue) => x.getN.toInt
  implicit val attributeValueToShort  = (x: AttributeValue) => x.getN.toShort
  implicit val attributeValueToByte   = (x: AttributeValue) => x.getN.toByte

  implicit val attributeValueToByteArray = (x: AttributeValue) => x.getB.array

}
