package aws.wrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._

object QueryCondition {

  def equals[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.EQ)
    .withAttributeValueList(conv(attrVal))

  def lessThan[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.LT)
    .withAttributeValueList(conv(attrVal))

  def lessThanOrEqual[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.LE)
    .withAttributeValueList(conv(attrVal))

  def greaterThan[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.GT)
    .withAttributeValueList(conv(attrVal))

  def greaterThanOrEqual[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.GE)
    .withAttributeValueList(conv(attrVal))

  def beginsWith(attrVal: String): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
    .withAttributeValueList(new AttributeValue().withS(attrVal))

  def beginsWith(attrVal: Array[Byte]): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
    .withAttributeValueList(new AttributeValue().withB(java.nio.ByteBuffer.wrap(attrVal)))

  def between[K](lower: K, upper: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.BETWEEN)
    .withAttributeValueList(conv(lower), conv(upper))

}
