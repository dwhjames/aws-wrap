
package aws.wrap
package dynamodb

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

/**
  * A lightweight wrapper for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonDynamoDBAsyncClient]].
  */
trait AmazonDynamoDBScalaClient {

  /**
    * An abstract [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonDynamoDBAsyncClient]].
    */
  val client: AmazonDynamoDBAsyncClient

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#batchGetItem(com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest) AWS Java SDK]]
    */
  def batchGetItem(
    batchGetItemRequest: BatchGetItemRequest
  ): Future[BatchGetItemResult] =
    wrapAsyncMethod(client.batchGetItemAsync, batchGetItemRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#batchGetItem(com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest) AWS Java SDK]]
    */
  def batchGetItem(
    requestItems: Map[String, KeysAndAttributes]
  ): Future[BatchGetItemResult] =
    batchGetItem(
      new BatchGetItemRequest()
      .withRequestItems(requestItems.asJava)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#batchWriteItem(com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest) AWS Java SDK]]
    */
  def batchWriteItem(
    batchWriteItemRequest: BatchWriteItemRequest
  ): Future[BatchWriteItemResult] =
    wrapAsyncMethod(client.batchWriteItemAsync, batchWriteItemRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#batchWriteItem(com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest) AWS Java SDK]]
    */
  def batchWriteItem(
    requestItems: Map[String, Seq[WriteRequest]]
  ): Future[BatchWriteItemResult] =
    batchWriteItem(
      new BatchWriteItemRequest()
      .withRequestItems(requestItems.map(p => (p._1, p._2.asJava)).asJava)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#createTable(com.amazonaws.services.dynamodbv2.model.CreateTableRequest) AWS Java SDK]]
    */
  def createTable(
    createTableRequest: CreateTableRequest
  ): Future[CreateTableResult] =
    wrapAsyncMethod(client.createTableAsync, createTableRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#createTable(com.amazonaws.services.dynamodbv2.model.CreateTableRequest) AWS Java SDK]]
    */
  def createTable(
    tableName:             String,
    provisionedThroughput: ProvisionedThroughput,
    attributeDefinitions:  Seq[AttributeDefinition],
    keySchema:             Seq[KeySchemaElement],
    localSecondaryIndexes: Seq[LocalSecondaryIndex] = Seq.empty
  ): Future[CreateTableResult] =
    createTable(
      new CreateTableRequest()
      .withTableName(tableName)
      .withProvisionedThroughput(provisionedThroughput)
      .withAttributeDefinitions(attributeDefinitions.asJavaCollection)
      .withKeySchema(keySchema.asJavaCollection)
      .withLocalSecondaryIndexes(localSecondaryIndexes.asJavaCollection)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#deleteItem(com.amazonaws.services.dynamodbv2.model.DeleteItemRequest) AWS Java SDK]]
    */
  def deleteItem(
    deleteItemRequest: DeleteItemRequest
  ): Future[DeleteItemResult] =
    wrapAsyncMethod(client.deleteItemAsync, deleteItemRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#deleteItem(com.amazonaws.services.dynamodbv2.model.DeleteItemRequest) AWS Java SDK]]
    */
  def deleteItem(
    tableName: String,
    key:       Map[String, AttributeValue]
  ): Future[DeleteItemResult] =
    deleteItem(new DeleteItemRequest(tableName, key.asJava))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#deleteTable(com.amazonaws.services.dynamodbv2.model.DeleteTableRequest) AWS Java SDK]]
    */
  def deleteTable(
    deleteTableRequest: DeleteTableRequest
  ): Future[DeleteTableResult] =
    wrapAsyncMethod(client.deleteTableAsync, deleteTableRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#deleteTable(com.amazonaws.services.dynamodbv2.model.DeleteTableRequest) AWS Java SDK]]
    */
  def deleteTable(
    tableName: String
  ): Future[DeleteTableResult] =
    deleteTable(new DeleteTableRequest(tableName))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#describeTable(com.amazonaws.services.dynamodbv2.model.DescribeTableRequest) AWS Java SDK]]
    */
  def describeTable(
    describeTableRequest: DescribeTableRequest
  ): Future[DescribeTableResult] =
    wrapAsyncMethod(client.describeTableAsync, describeTableRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#describeTable(com.amazonaws.services.dynamodbv2.model.DescribeTableRequest) AWS Java SDK]]
    */
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

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#getItem(com.amazonaws.services.dynamodbv2.model.GetItemRequest) AWS Java SDK]]
    */
  def getItem(
    getItemRequest: GetItemRequest
  ): Future[GetItemResult] =
    wrapAsyncMethod(client.getItemAsync, getItemRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#getItem(com.amazonaws.services.dynamodbv2.model.GetItemRequest) AWS Java SDK]]
    */
  def getItem(
    tableName:       String,
    key:             Map[String, AttributeValue],
    attributesToGet: Iterable[String] = Iterable.empty,
    consistentRead:  Boolean          = false
  ): Future[GetItemResult] =
    getItem(
      new GetItemRequest(tableName, key.asJava)
      .withAttributesToGet(attributesToGet.asJavaCollection)
      .withConsistentRead(consistentRead)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#listTables(com.amazonaws.services.dynamodbv2.model.ListTablesRequest) AWS Java SDK]]
    */
  def listTables(
    listTablesRequest: ListTablesRequest
  ): Future[ListTablesResult] =
    wrapAsyncMethod(client.listTablesAsync, listTablesRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#listTables(com.amazonaws.services.dynamodbv2.model.ListTablesRequest) AWS Java SDK]]
    */
  def listTables(): Future[ListTablesResult] =
    listTables(new ListTablesRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#putItem(com.amazonaws.services.dynamodbv2.model.PutItemRequest) AWS Java SDK]]
    */
  def putItem(
    putItemRequest: PutItemRequest
  ): Future[PutItemResult] =
    wrapAsyncMethod(client.putItemAsync, putItemRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#putItem(com.amazonaws.services.dynamodbv2.model.PutItemRequest) AWS Java SDK]]
    */
  def putItem(
    tableName: String,
    item:      Map[String, AttributeValue]
  ): Future[PutItemResult] =
    putItem(new PutItemRequest(tableName, item.asJava))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#query(com.amazonaws.services.dynamodbv2.model.QueryRequest) AWS Java SDK]]
    */
  def query(
    queryRequest: QueryRequest
  ): Future[QueryResult] =
    wrapAsyncMethod(client.queryAsync, queryRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#query(com.amazonaws.services.dynamodbv2.model.QueryRequest) AWS Java SDK]]
    */
  def query(
    tableName:     String,
    keyConditions: Map[String, Condition]
  ): Future[QueryResult] =
    query(
      new QueryRequest()
      .withTableName(tableName)
      .withKeyConditions(keyConditions.asJava)
    )

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#scan(com.amazonaws.services.dynamodbv2.model.ScanRequest) AWS Java SDK]]
    */
  def scan(
    scanRequest: ScanRequest
  ): Future[ScanResult] =
    wrapAsyncMethod(client.scanAsync, scanRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#scan(com.amazonaws.services.dynamodbv2.model.ScanRequest) AWS Java SDK]]
    */
  def scan(
    tableName:  String,
    scanFilter: Map[String, Condition] = Map.empty
  ): Future[ScanResult] =
    scan(
      new ScanRequest(tableName)
      .withScanFilter(scanFilter.asJava)
    )

  def shutdown(): Unit =
    client.shutdown()

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#updateItem(com.amazonaws.services.dynamodbv2.model.UpdateItemRequest) AWS Java SDK]]
    */
  def updateItem(
    updateItemRequest: UpdateItemRequest
  ): Future[UpdateItemResult] =
    wrapAsyncMethod(client.updateItemAsync, updateItemRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#updateItem(com.amazonaws.services.dynamodbv2.model.UpdateItemRequest) AWS Java SDK]]
    */
  def updateItem(
    tableName:        String,
    key:              Map[String, AttributeValue],
    attributeUpdates: Map[String, AttributeValueUpdate]
  ): Future[UpdateItemResult] =
    updateItem(new UpdateItemRequest(tableName, key.asJava, attributeUpdates.asJava))

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#updateTable(com.amazonaws.services.dynamodbv2.model.UpdateTableRequest) AWS Java SDK]]
    */
  def updateTable(
    updateTableRequest: UpdateTableRequest
  ): Future[UpdateTableResult] =
    wrapAsyncMethod(client.updateTableAsync, updateTableRequest)

  /**
    * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDB.html#updateTable(com.amazonaws.services.dynamodbv2.model.UpdateTableRequest) AWS Java SDK]]
    */
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

/**
  * A factory for [[AmazonDynamoDBScalaClient]] instances.
  */
object AmazonDynamoDBScalaClient {

  private class AmazonDynamoDBScalaClientImpl(override val client: AmazonDynamoDBAsyncClient) extends AmazonDynamoDBScalaClient

  /**
    * Construct an [[AmazonDynamoDBScalaClient]] instance from an
    * [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonDynamoDBAsyncClient]]
    * instance.
    *
    * A factory method for [[AmazonDynamoDBScalaClient]] instances.
    *
    * @param client
    *     a [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBAsyncClient.html AmazonDynamoDBAsyncClient]] client.
    * @return a new [[AmazonDynamoDBScalaClient]] instance.
    */
  def fromAsyncClient(client: AmazonDynamoDBAsyncClient): AmazonDynamoDBScalaClient =
    new AmazonDynamoDBScalaClientImpl(client)
}
