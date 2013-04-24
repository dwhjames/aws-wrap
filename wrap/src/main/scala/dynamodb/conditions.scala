package aws.wrap.dynamodb

import scala.collection.JavaConverters._

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition}

private[dynamodb] trait AttributeConditions {

  def equalTo[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
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

object QueryCondition extends AttributeConditions

object ScanCondition extends AttributeConditions {

  def notEqualTo[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NE)
    .withAttributeValueList(conv(attrVal))

  val null: Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NULL)

  val notNull: Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NOT_NULL)

  def contains[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.CONTAINS)
    .withAttributeValueList(conv(attrVal))

  def doesNotContain[K](attrVal: K)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.NOT_CONTAINS)
    .withAttributeValueList(conv(attrVal))

  def in[K](attrVals: K*)(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.IN)
    .withAttributeValueList(attrVals.map(conv).asJavaCollection)

  def in[K](attrVals: Set[K])(implicit conv: K => AttributeValue): Condition =
    new Condition()
    .withComparisonOperator(ComparisonOperator.IN)
    .withAttributeValueList(attrVals.map(conv).asJavaCollection)
}
