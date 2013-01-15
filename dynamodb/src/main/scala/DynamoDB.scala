/*
 * Copyright 2012 Pellucid and Zenexity
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aws.dynamodb

import scala.concurrent.{ Future, ExecutionContext }

import play.api.libs.ws._
import play.api.libs.ws.WS._
import play.api.libs.json._

import aws.core._
import aws.core.Types._
import aws.core.parsers._
import aws.core.utils._
import aws.core.signature._

import aws.dynamodb.JsonFormats._
import aws.dynamodb.DDBParsers._

trait DynamoDBLayer{ self: AWS => 
  
  object DynamoDB {

    import aws.dynamodb._
    import ReturnValues.ReturnValue

    private def request(operation: String,
                        body: JsValue)(implicit region: DDBRegion): Future[Response] = {
      val requestTime = new java.util.Date()
      val headers = Seq(
        "host" -> region.host,
        "x-amz-date" -> isoDateFormat(requestTime),
        "x-amz-target" -> ("DynamoDB_" + V4.VERSION + "." + operation),
        "Content-Type" -> "application/x-amz-json-1.0")
      val allHeaders = headers ++ Seq(
        "Authorization" -> V4.authorizationHeader(requestTime, headers, Nil, body.toString))

      WS.url("https://" + region.host + "/").withHeaders(allHeaders: _*).post(body.toString)
    }

    private def tryParse[T](resp: Response)(implicit p: Parser[SimpleResult[T]]) = {
      Parser.parse[SimpleResult[T]](resp).fold(
        e => throw new RuntimeException(e),
        identity)
    }

    private def post[T](operation: String,
                        body: JsValue)(implicit p: Parser[T], region: DDBRegion): Future[SimpleResult[T]] = {
      request(operation, body).map(r => tryParse[T](r))
    }

    /**
     * Returns a sequence of all the tables names associated with the current account and endpoint.
     *
     * The ListTables operation returns all of the table names associated with the account making the request, for the endpoint that receives the request.
     *
     * @param limit A number of maximum table names to return
     * @param exclusiveStartTableName The name of the table that starts the list. If you already ran a ListTables operation and received an LastEvaluatedTableName value in the response, use that value here to continue the list.
     */
    def listTables(limit: Option[Int] = None,
                   exclusiveStartTableName: Option[String] = None)(implicit region: DDBRegion): Future[SimpleResult[Seq[String]]] = {
      val data = (
        limit.map("Limit" -> Json.toJson(_))
        ++ exclusiveStartTableName.map("ExclusiveStartTableName" -> Json.toJson(_))).toMap
      post[Seq[String]]("ListTables", Json.toJson(data))
    }

    /**
     * The CreateTable operation adds a new table to your account. The table name must be unique among those associated with the AWS Account issuing the request,
     * and the AWS region that receives the request (such as dynamodb.us-east-1.amazonaws.com).
     *
     * The CreateTable operation triggers an asynchronous workflow to begin creating the table. Amazon DynamoDB immediately returns the state of the table ([[Status.CREATING CREATING]])
     * until the table is in the [[Status.ACTIVE ACTIVE]] state. Once the table is in the [[Status.ACTIVE ACTIVE]] state, you can perform data plane operations.
     * Use [[describeTable]] to check the status of the table.
     *
     * @param tableName The name of the table to create.
     *                  Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (dash), and '.' (dot).
     *                  Names can be between 3 and 255 characters long.
     * @param keySchema the primary key structure for the table. See [[PrimaryKey]] for more information.
     */
    def createTable(tableName: String,
                    keySchema: PrimaryKey,
                    provisionedThroughput: ProvisionedThroughput)(implicit region: DDBRegion): Future[SimpleResult[TableDescription]] = {
      val body = Json.obj(
        "TableName" -> tableName,
        "KeySchema" -> keySchema,
        "ProvisionedThroughput" -> provisionedThroughput)
      post[TableDescription]("CreateTable", body)
    }

    /**
     * Updates the provisioned throughput for the given table. Setting the throughput for a table
     * helps you manage performance and is part of the provisioned throughput feature of Amazon DynamoDB.
     * For more information, see [[http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/WorkingWithDDTables.html#ProvisionedThroughput Specifying Read and Write Requirements (Provisioned Throughput)]].
     *
     * The provisioned throughput values can be upgraded or downgraded based on the maximums and minimums listed in Limits in Amazon DynamoDB.
     *
     * The table must be in the [[Status.ACTIVE ACTIVE]] state for this operation to succeed.
     * UpdateTable is an asynchronous operation; while executing the operation, the table is in the [[Status.UPDATING UPDATING]] state.
     * While the table is in the [[Status.UPDATING UPDATING]] state, the table still has the provisioned throughput from before the call.
     * The new provisioned throughput setting is in effect only when the table returns to the [[Status.ACTIVE ACTIVE]] state after the UpdateTable operation.
     *
     * @param tableName
     */
    def updateTable(tableName: String,
                    provisionedThroughput: ProvisionedThroughput)(implicit region: DDBRegion): Future[SimpleResult[TableDescription]] = {
      val body = Json.obj(
        "TableName" -> tableName,
        "ProvisionedThroughput" -> provisionedThroughput)
      post[TableDescription]("UpdateTable", body)
    }

    /**
     * The DeleteTable operation deletes a table and all of its items.
     * After a DeleteTable request, the specified table is in the [[Status.DELETING DELETING]] state until Amazon DynamoDB completes
     * the deletion.
     *
     *  - If the table is in the [[Status.ACTIVE ACTIVE]] state, you can delete it.
     *  - If a table is in [[Status.CREATING CREATING]] or [[Status.UPDATING UPDATING]] states,
     * then Amazon DynamoDB returns a ResourceInUseException error.
     *  - If the specified table does not exist, Amazon DynamoDB returns a ResourceNotFoundException.
     *  - If table is already in the [[Status.DELETING DELETING]] state, no error is returned.
     *
     * Amazon DynamoDB might continue to accept data plane operation requests, such as [[getItem]] and [[putItem]],
     * on a table in the DELETING state until the table deletion is complete.
     *
     * @param tableName
     */
    def deleteTable(tableName: String)(implicit region: DDBRegion): Future[EmptySimpleResult] = {
      val body = Json.obj("TableName" -> tableName)
      post[Unit]("DeleteTable", body)
    }

    /**
     * Returns information about the table, including the current status of the table,
     * the primary key schema and when the table was created.
     *
     * DescribeTable results are eventually consistent.
     * If you use DescribeTable too early in the process of creating a table, Amazon DynamoDB returns a ResourceNotFoundException.
     * If you use DescribeTable too early in the process of updating a table, the new values might not be immediately available.
     *
     * @param tableName
     */
    def describeTable(tableName: String)(implicit region: DDBRegion): Future[SimpleResult[TableDescription]] = {
      val body = Json.obj("TableName" -> JsString(tableName))
      post[TableDescription]("DescribeTable", body)
    }

    /**
     * Creates a new item, or replaces an old item with a new item (including all the attributes).
     * If an item already exists in the specified table with the same primary key, the new item completely replaces the existing item. You can perform a conditional put (insert a new item if one with the specified primary key doesn't exist), or replace an existing item if it has certain attribute values.
     *
     * @param tableName
     * @param item The [[Item]] to put. Must include the primary key values that define the item.
     * Other attribute name-value pairs can be provided for the item. For more information about primary keys, see [[PrimaryKey]].
     */
    def putItem(tableName: String,
                item: Item,
                expected: Map[String, Expected] = Map.empty,
                returnValues: ReturnValue = ReturnValues.NONE)(implicit region: DDBRegion): Future[SimpleResult[ItemResponse]] = {
      val body = Json.obj(
        "TableName" -> JsString(tableName),
        "Item" -> Json.toJson(item),
        "Expected" -> Json.toJson(expected),
        "ReturnValues" -> JsString(returnValues.toString))
      post[ItemResponse]("PutItem", body)
    }

    /**
     * Deletes a single item in a table by primary key.
     * You can perform a conditional delete operation that deletes the item if it exists,
     * or if it has an expected attribute value.
     *
     * Unless you specify conditions, the DeleteItem is an idempotent operation; running it multiple times on the same item or attribute does not result in an error response.
     *
     * @param tableName
     * @param key the primary key that defines the item. See [[PrimaryKey]] for more information.
     * @param expected Designates an attribute for a conditional delete. If empty, all the attributes for the item are deleted.
     *                 If not, it allows deleting items and attributes if specific conditions are met.
     *                 If the conditions are met, Amazon DynamoDB performs the delete. Otherwise, the item is not deleted.
     * @param returnValues Use this parameter if you want to get the attribute name-value pairs before they were deleted.
     * If [[ReturnValues.ALL_OLD]] is specified, the content of the old item is returned.
     * If [[ReturnValues.NONE]] is provided (the default value), nothing is returned.
     */
    def deleteItem(tableName: String,
                   key: KeyValue,
                   expected: Map[String, Expected] = Map.empty,
                   returnValues: ReturnValue = ReturnValues.NONE)(implicit region: DDBRegion): Future[SimpleResult[ItemResponse]] = {
      val body = Json.obj(
        "TableName" -> JsString(tableName),
        "Key" -> Json.toJson(key),
        "Expected" -> Json.toJson(expected),
        "ReturnValues" -> JsString(returnValues.toString))
      post[ItemResponse]("DeleteItem", body)
    }

    /**
     * The GetItem operation returns a set of Attributes for an item that matches the primary key.
     *
     * The GetItem operation provides an eventually consistent read by default.
     * If eventually consistent reads are not acceptable for your application, use ConsistentRead.
     * Although this operation might take longer than a standard read, it always returns the last updated value.
     * For more information, see [[http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/APISummary.html#DataReadConsistency Data Read and Consistency Considerations]].
     */
    def getItem(tableName: String,
                key: KeyValue,
                attributesToGet: Seq[String] = Nil,
                consistentRead: Boolean = false)(implicit region: DDBRegion): Future[SimpleResult[ItemResponse]] = {
      val body = Json.obj(
        "TableName" -> JsString(tableName),
        "Key" -> Json.toJson(key),
        "ConsistentRead" -> JsBoolean(consistentRead)) ++ (attributesToGet match {
          case Nil => Json.obj()
          case _ => Json.obj("AttributesToGet" -> Json.toJson(attributesToGet))
        })
      post[ItemResponse]("GetItem", body)
    }

    def updateItem(tableName: String,
                   key: KeyValue,
                   attributeUpdates: Map[String, Update],
                   expected: Map[String, Expected] = Map.empty,
                   returnValues: ReturnValue = ReturnValues.NONE)(implicit region: DDBRegion): Future[SimpleResult[ItemResponse]] = {
      val body = Json.obj(
        "TableName" -> JsString(tableName),
        "Key" -> Json.toJson(key),
        "Expected" -> Json.toJson(expected),
        "AttributeUpdates" -> Json.toJson(attributeUpdates),
        "ReturnValues" -> JsString(returnValues.toString))
      post[ItemResponse]("UpdateItem", body)
    }

    def query(query: Query)(implicit region: DDBRegion): Future[SimpleResult[QueryResponse]] = {
      post[QueryResponse]("Query", Json.toJson(query))
    }

    def scan(tableName: String,
             attributesToGet: Seq[String] = Nil,
             limit: Option[Long] = None,
             count: Boolean = false,
             scanFilter: Option[KeyCondition] = None,
             exclusiveStartKey: Option[PrimaryKey] = None)(implicit region: DDBRegion): Future[SimpleResult[QueryResponse]] = {
      val data = Seq(
        "TableName" -> JsString(tableName),
        "Count" -> JsBoolean(count)) ++
        Some(attributesToGet).filterNot(_.isEmpty).map("AttributesToGet" -> Json.toJson(_)) ++
        limit.map("Limit" -> Json.toJson(_)) ++
        scanFilter.map("ScanFilter" -> Json.toJson(_)) ++
        exclusiveStartKey.map("ExclusiveStartKey" -> Json.toJson(_))

      post[QueryResponse]("Scan", Json.toJson(data.toMap))
    }

    def batchWriteItem(requestItems: Map[String, Seq[WriteRequest]])(implicit region: DDBRegion): Future[SimpleResult[BatchWriteResponse]] = {
      post[BatchWriteResponse]("BatchWriteItem", Json.obj(
        "RequestItems" -> Json.toJson(requestItems)))
    }

    def batchGetItem(requestItems: Seq[GetRequest])(implicit region: DDBRegion): Future[SimpleResult[BatchGetResponse]] = {
      post[BatchGetResponse]("BatchGetItem", Json.obj(
        "RequestItems" -> Json.toJson(requestItems)))
    }

  }
}