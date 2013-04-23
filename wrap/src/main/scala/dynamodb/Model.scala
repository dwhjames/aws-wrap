package aws.wrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2._

object Model {

  sealed trait KeyCondition {
    def toCondition: Condition
  }

  case class EqualTo(value: Any) extends KeyCondition {
    def toCondition =
      new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(any2AttributeValue(value))
  }

  case class Between(lowerBound: Any, upperBound: Any) extends KeyCondition {
    def toCondition =
      new Condition()
      .withComparisonOperator(ComparisonOperator.BETWEEN)
      .withAttributeValueList(any2AttributeValue(lowerBound), any2AttributeValue(upperBound))
  }

  case class Query(
    hashKeyValue: Any,
    rangeKeyCondition: Option[KeyCondition] = None
  )
}
