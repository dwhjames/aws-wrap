package aws.wrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._

object Schema {

  def provisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long) =
    new ProvisionedThroughput()
      .withReadCapacityUnits(readCapacityUnits)
      .withWriteCapacityUnits(writeCapacityUnits)

  def stringAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.S)

  def numberAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.N)

  def binaryAttribute(attributeName: String): AttributeDefinition =
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(ScalarAttributeType.B)

  def hashKey(attributeName: String): KeySchemaElement =
    new KeySchemaElement()
      .withAttributeName(attributeName)
      .withKeyType(KeyType.HASH)

  def rangeKey(attributeName: String): KeySchemaElement =
    new KeySchemaElement()
      .withAttributeName(attributeName)
      .withKeyType(KeyType.RANGE)

}
