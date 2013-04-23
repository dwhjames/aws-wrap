package aws.wrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2._

object Model {

  sealed trait KeyCondition {
    def toJava: Condition
  }

  case class EqualTo(value: Any) extends KeyCondition {
    def toJava =
      new Condition()
        .withComparisonOperator(ComparisonOperator.EQ.toString())
        .withAttributeValueList(any2AttributeValue(value))
  }
  case class LessThan(value: Any, orEqual: Boolean) extends KeyCondition {
    def toJava =
      new Condition()
        .withComparisonOperator(ComparisonOperator.LT.toString())
        .withAttributeValueList(any2AttributeValue(value))
  }
  case class GreaterThan(value: Any, orEqual: Boolean) extends KeyCondition {
    def toJava =
      new Condition()
        .withComparisonOperator(ComparisonOperator.GT.toString())
        .withAttributeValueList(any2AttributeValue(value))
  }
  case class BeginsWith(value: Any) extends KeyCondition {
    def toJava =
      new Condition()
        .withComparisonOperator(ComparisonOperator.BEGINS_WITH.toString())
        .withAttributeValueList(any2AttributeValue(value))
  }
  case class Between(lowerBound: Any, upperBound: Any) extends KeyCondition {
    def toJava =
      new Condition()
        .withComparisonOperator(ComparisonOperator.BETWEEN.toString())
        .withAttributeValueList(any2AttributeValue(lowerBound), any2AttributeValue(upperBound))
  }

  case class Query(
    hashKeyValue: Any,
    rangeKeyCondition: Option[(String, KeyCondition)] = None,
    tableName: String = "",
    attributesToGet: Option[Seq[String]] = None,
    limit: Option[Long] = None,
    consistentRead: Boolean = false,
    count: Boolean = false,
    scanIndexForward: Boolean = true,
    exclusiveStartKey: Option[PrimaryKey] = None)
}
