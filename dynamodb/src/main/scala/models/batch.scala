package aws.dynamodb.models

sealed trait WriteRequest

case class PutRequest(item: Map[String, DDBAttribute]) extends WriteRequest

case class DeleteRequest(key: Key) extends WriteRequest

case class BatchWriteResponse(responses: Seq[(String, QueryResponse)], unprocessed: Seq[(String, Seq[WriteRequest])])

case class GetRequest(keys: Seq[Key], attributesToGet: Seq[String] = Nil)

case class BatchGetResponse(responses: Seq[(String, QueryResponse)], unprocessed: Seq[(String, Seq[GetRequest])])

