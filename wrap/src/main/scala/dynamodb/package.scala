
package aws.wrap

import com.amazonaws.services.dynamodbv2.model._

package object dynamodb {

  def defineDynamoDBProvisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long) =
    new ProvisionedThroughput()
      .withReadCapacityUnits(readCapacityUnits)
      .withWriteCapacityUnits(writeCapacityUnits)

  def defineDynamoDBStringAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.S)

  def defineDynamoDBNumberAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.N)

  def defineDynamoDBBinaryAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.B)

  def defineDynamoDBHashKeySchemaElement(attributeName: String): KeySchemaElement =
    new KeySchemaElement()
      .withAttributeName(attributeName)
      .withKeyType(KeyType.HASH)

  def defineDynamoDBRangeKeySchemaElement(attributeName: String): KeySchemaElement =
    new KeySchemaElement()
      .withAttributeName(attributeName)
      .withKeyType(KeyType.RANGE)

  // string attribute value
  implicit val stringKeyValue = (x: String) => new AttributeValue().withS(x)

  // numeric attribute value
  implicit val doubleKeyValue = (x: Double) => new AttributeValue().withN(x.toString)
  implicit val floatKeyValue  = (x: Float)  => new AttributeValue().withN(x.toString)
  implicit val longKeyValue   = (x: Long)   => new AttributeValue().withN(x.toString)
  implicit val intKeyValue    = (x: Int)    => new AttributeValue().withN(x.toString)
  implicit val charKeyValue   = (x: Char)   => new AttributeValue().withN(x.toString)
  implicit val shortKeyValue  = (x: Short)  => new AttributeValue().withN(x.toString)
  implicit val byteKeyValue   = (x: Byte)   => new AttributeValue().withN(x.toString)

  // binary attribute value
  implicit val byteArrayKeyValue = (x: Array[Byte]) => new AttributeValue().withB(java.nio.ByteBuffer.wrap(x))

  private[dynamodb] def any2AttributeValue(x: Any): AttributeValue =
    x match {
      case x: String =>
        new AttributeValue().withS(x)
      case x: Array[Byte] =>
        new AttributeValue().withB(java.nio.ByteBuffer.wrap(x))
      case x: Double =>
        new AttributeValue().withN(x.toString)
      case x: Float =>
        new AttributeValue().withN(x.toString)
      case x: Long =>
        new AttributeValue().withN(x.toString)
      case x: Int =>
        new AttributeValue().withN(x.toString)
      case x: Char =>
        new AttributeValue().withN(x.toString)
      case x: Short =>
        new AttributeValue().withN(x.toString)
      case x: Byte =>
        new AttributeValue().withN(x.toString)
      case _ =>
        throw new IllegalArgumentException(s"The value $x is not a supported DynamoDB AttributeValue")
    }
}
