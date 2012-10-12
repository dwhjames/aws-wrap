package aws.dynamodb.models

sealed trait KeyCondition

object KeyCondition {

  case class EqualTo(value: DDBAttribute) extends KeyCondition

  case class LessThan(value: DDBAttribute, orEqual: Boolean) extends KeyCondition

  case class GreaterThan(value: DDBAttribute, orEqual: Boolean) extends KeyCondition

  case class BeginsWith(value: DDBAttribute) extends KeyCondition

  case class Between(lowerBound: DDBAttribute, upperBound: DDBAttribute) extends KeyCondition

}

case class Query(tableName: String,
                 hashKeyValue: DDBAttribute,
                 attributesToGet: Seq[String] = Nil,
                 limit: Option[Long] = None,
                 consistentRead: Boolean = false,
                 count: Boolean = false,
                 rangeKeyCondition: Option[KeyCondition] = None,
                 scanIndexForward: Boolean = true,
                 exclusiveStartKey: Option[PrimaryKey] = None)
