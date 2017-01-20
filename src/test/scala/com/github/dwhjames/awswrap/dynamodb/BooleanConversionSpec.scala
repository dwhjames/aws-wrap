package com.github.dwhjames.awswrap.dynamodb

import scala.collection.JavaConverters._
import org.scalatest.{FlatSpec, Matchers}

class BooleanConversionSpec extends FlatSpec with Matchers {

  it should "convert from DynamoDb Bool to Scala Boolean" in {
    val attributeValue = new AttributeValue().withBOOL(false)
    val bool = attributeValueToBoolean(attributeValue)
    bool shouldBe false
  }

  it should "convert from DynamoDb Bool Collection to Scala Boolean Set" in {
    val attributeValue = new AttributeValue().withBOOL(false)
    val attributeValueList = new AttributeValue().withL(Seq(attributeValue).asJava)
    val bool = attributeValueToBooleanSet(attributeValueList)
    bool shouldBe Set(false)
  }
}
