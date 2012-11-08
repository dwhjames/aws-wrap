package aws.dynamodb

sealed trait WriteRequest

case class PutRequest(item: Map[String, DDBAttribute]) extends WriteRequest

case class DeleteRequest(key: KeyValue) extends WriteRequest

/**
 * Used for the response to [[DynamoDB.batchWriteItem]]
 * @param successful requests: a Map of [[QueryResponses]] per table
 * @param unsuccessful requests, per table
 */
case class BatchWriteResponse(responses: Seq[(String, QueryResponse)], unprocessed: Seq[(String, Seq[WriteRequest])])

case class GetRequest(keys: Seq[KeyValue], attributesToGet: Seq[String] = Nil, consistentRead: Boolean = false)

/**
 * Used for the response to [[DynamoDB.batchGetItem]]
 * @param successful requests: a Map of [[QueryResponses]] per table
 * @param unsuccessful requests, per table
 */
case class BatchGetResponse(responses: Seq[(String, QueryResponse)], unprocessed: Seq[(String, Seq[GetRequest])])

