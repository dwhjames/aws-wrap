
package aws.wrap

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.dynamodb._
import com.amazonaws.services.dynamodb.model._

trait AmazonDynamoDBScalaClient {

  val client: AmazonDynamoDBAsyncClient

  def batchGetItem(
    batchGetItemRequest: BatchGetItemRequest
  ): Future[BatchGetItemResult] =
    wrapAsyncMethod(client.batchGetItemAsync, batchGetItemRequest)

  def batchGetItem(
    requestItems: Map[String, KeysAndAttributes]
  ): Future[BatchGetItemResult] =
    batchGetItem(
      new BatchGetItemRequest()
      .withRequestItems(requestItems.asJava)
    )

  def batchWriteItem(
    batchWriteItemRequest: BatchWriteItemRequest
  ): Future[BatchWriteItemResult] =
    wrapAsyncMethod(client.batchWriteItemAsync, batchWriteItemRequest)

  def batchWriteItem(
    requestItems: Map[String, Seq[WriteRequest]]
  ): Future[BatchWriteItemResult] =
    batchWriteItem(
      new BatchWriteItemRequest()
      .withRequestItems(requestItems.map(p => (p._1, p._2.asJava)).asJava)
    )

  def createTable(
    createTableRequest: CreateTableRequest
  ): Future[CreateTableResult] =
    wrapAsyncMethod(client.createTableAsync, createTableRequest)

  def createTable(
    tableName:             String,
    keySchema:             KeySchema,
    provisionedThroughput: ProvisionedThroughput
  ): Future[CreateTableResult] =
    createTable(
      new CreateTableRequest(tableName, keySchema)
      .withProvisionedThroughput(provisionedThroughput)
    )

  def deleteItem(
    deleteItemRequest: DeleteItemRequest
  ): Future[DeleteItemResult] =
    wrapAsyncMethod(client.deleteItemAsync, deleteItemRequest)

  def deleteItem(
    tableName: String,
    key:       Key
  ): Future[DeleteItemResult] =
    deleteItem(new DeleteItemRequest(tableName, key))

  def deleteTable(
    deleteTableRequest: DeleteTableRequest
  ): Future[DeleteTableResult] =
    wrapAsyncMethod(client.deleteTableAsync, deleteTableRequest)

  def deleteTable(
    tableName: String
  ): Future[DeleteTableResult] =
    deleteTable(new DeleteTableRequest(tableName))

  def describeTable(
    describeTableRequest: DescribeTableRequest
  ): Future[DescribeTableResult] =
    wrapAsyncMethod(client.describeTableAsync, describeTableRequest)

  def describeTable(
    tableName: String
  ): Future[DescribeTableResult] =
    describeTable(
      new DescribeTableRequest()
      .withTableName(tableName)
    )

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

  def getExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(client.getExecutorService())

  def getItem(
    getItemRequest: GetItemRequest
  ): Future[GetItemResult] =
    wrapAsyncMethod(client.getItemAsync, getItemRequest)

  def getItem(
    tableName:       String,
    key:             Key,
    attributesToGet: Iterable[String] = Iterable.empty,
    consistentRead:  Boolean          = false
  ): Future[GetItemResult] =
    getItem(
      new GetItemRequest(tableName, key)
      .withAttributesToGet(attributesToGet.asJavaCollection)
      .withConsistentRead(consistentRead)
    )

  def listTables(
    listTablesRequest: ListTablesRequest
  ): Future[ListTablesResult] =
    wrapAsyncMethod(client.listTablesAsync, listTablesRequest)

  def listTables(): Future[ListTablesResult] =
    listTables(new ListTablesRequest)

  def putItem(
    putItemRequest: PutItemRequest
  ): Future[PutItemResult] =
    wrapAsyncMethod(client.putItemAsync, putItemRequest)

  def putItem(
    tableName: String,
    item:      Map[String, AttributeValue]
  ): Future[PutItemResult] =
    putItem(new PutItemRequest(tableName, item.asJava))

  def query(
    queryRequest: QueryRequest
  ): Future[QueryResult] =
    wrapAsyncMethod(client.queryAsync, queryRequest)

  def query(
    tableName:    String,
    hashKeyValue: AttributeValue
  ): Future[QueryResult] =
    query(new QueryRequest(tableName, hashKeyValue))

  def scan(
    scanRequest: ScanRequest
  ): Future[ScanResult] =
    wrapAsyncMethod(client.scanAsync, scanRequest)

  def scan(
    tableName: String
  ): Future[ScanResult] =
    scan(new ScanRequest(tableName))

  def shutdown(): Unit =
    client.shutdown()

  def updateItem(
    updateItemRequest: UpdateItemRequest
  ): Future[UpdateItemResult] =
    wrapAsyncMethod(client.updateItemAsync, updateItemRequest)

  def updateItem(
    tableName:        String,
    key:              Key,
    attributeUpdates: Map[String, AttributeValueUpdate]
  ): Future[UpdateItemResult] =
    updateItem(new UpdateItemRequest(tableName, key, attributeUpdates.asJava))

  def updateTable(
    updateTableRequest: UpdateTableRequest
  ): Future[UpdateTableResult] =
    wrapAsyncMethod(client.updateTableAsync, updateTableRequest)

  def updateTable(
    tableName:             String,
    provisionedThroughput: ProvisionedThroughput
  ): Future[UpdateTableResult] =
    updateTable(
      new UpdateTableRequest()
      .withTableName(tableName)
      .withProvisionedThroughput(provisionedThroughput)
    )
}

object AmazonDynamoDBScalaClient {

  private class AmazonDynamoDBScalaClientImpl(override val client: AmazonDynamoDBAsyncClient) extends AmazonDynamoDBScalaClient

  def fromAsyncClient(client: AmazonDynamoDBAsyncClient): AmazonDynamoDBScalaClient =
    new AmazonDynamoDBScalaClientImpl(client)
}
