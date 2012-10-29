package aws.dynamodb

sealed trait KeyCondition

object KeyCondition {

  case class EqualTo(value: DDBAttribute) extends KeyCondition

  case class LessThan(value: DDBAttribute, orEqual: Boolean) extends KeyCondition

  case class GreaterThan(value: DDBAttribute, orEqual: Boolean) extends KeyCondition

  case class BeginsWith(value: DDBAttribute) extends KeyCondition

  case class Between(lowerBound: DDBAttribute, upperBound: DDBAttribute) extends KeyCondition

}

/**
 * A DynamoDB Query.
 *
 * @param tableName The name of the table containing the requested items.
 * @param hashKeyValue Attribute value of the hash component of the composite primary key.
 * @param attributesToGet Array of Attribute names. If Nil then all attributes will be returned.
 *                        If some attributes are not found, they will not appear in the result.
 * @param limit The maximum number of items to return (not necessarily the number of matching items).
 *              If Amazon DynamoDB processes the number of items up to the limit while querying the table,
 *              it stops the query and returns the matching values up to that point,
 *              and a `LastEvaluatedKey` to apply in a subsequent operation to continue the query.
 *              Also, if the result set size exceeds 1MB before Amazon DynamoDB hits this limit,
 *              it stops the query and returns the matching values, and a `LastEvaluatedKey` to apply
 *              in a subsequent operation to continue the query.
 * @param consistentRead If set to `true`, then a consistent read is issued, otherwise eventually consistent is used.
 * @param count If set to true, Amazon DynamoDB returns a total number of items that match the query parameters,
 *              instead of a list of the matching items and their attributes. You can apply the Limit parameter to
 *              count-only queries.
 *              Do not set Count to true while providing a list of AttributesToGet;
 *              otherwise, Amazon DynamoDB returns a validation error.
 *              For more information, see [[http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Count Count and ScannedCount]].
 * @param rangeKeyCondition The attribute values and comparison operators to use for the query.
 *                          If `None`, Amazon DynamoDB returns all items with the specified hash key element value.
 * @param scanIndexForward Specifies ascending or descending traversal of the index.
 *                         Amazon DynamoDB returns results reflecting the requested order determined by the range key,
 *                         based on ASCII character code values. Defaults to `true` (ascending).
 * @param exclusiveStartKey Primary key of the item from which to continue an earlier query. An earlier query might provide
 *                          this value as the LastEvaluatedKey if that query operation was interrupted before completing the query;
 *                          either because of the result set size or the Limit parameter.
 *                          The `LastEvaluatedKey` can be passed back in a new query request to continue the operation
 *                          from that point.
 *
 */
case class Query(tableName: String,
                 hashKeyValue: DDBAttribute,
                 attributesToGet: Seq[String] = Nil,
                 limit: Option[Long] = None,
                 consistentRead: Boolean = false,
                 count: Boolean = false,
                 rangeKeyCondition: Option[KeyCondition] = None,
                 scanIndexForward: Boolean = true,
                 exclusiveStartKey: Option[PrimaryKey] = None)
