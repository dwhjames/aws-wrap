package aws.dynamodb

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import play.api.libs.ws._
import play.api.libs.ws.WS._
import play.api.libs.json._

import aws.core._
import aws.core.Types._
import aws.core.parsers._
import aws.core.utils._
import aws.core.signature._

import aws.dynamodb.models._
import aws.dynamodb.DDBParsers._

object DynamoDB {

  import aws.dynamodb._

  import DDBRegion.DEFAULT

  private def request(operation: String,
                      body: JsValue)(implicit region: AWSRegion): Future[Response] = {
    val requestTime = new java.util.Date()
    val headers = Seq(
      "host" -> region.host,
      "x-amz-date" -> AWS.isoDateFormat(requestTime),
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
                      body: JsValue)(implicit p: Parser[T], region: AWSRegion): Future[SimpleResult[T]] = {
    request(operation, body).map(r => tryParse[T](r))
  }

  def listTables(limit: Option[Int] = None,
                 exclusiveStartTableName: Option[String] = None)(implicit region: AWSRegion): Future[SimpleResult[Seq[String]]] = {
    val data = (
      limit.map("Limit" -> Json.toJson(_))
      ++ exclusiveStartTableName.map("ExclusiveStartTableName" -> Json.toJson(_))).toMap
    post[Seq[String]]("ListTables", Json.toJson(data))
  }

  def createTable(tableName: String,
                  keySchema: KeySchema,
                  provisionedThroughput: ProvisionedThroughput)(implicit region: AWSRegion): Future[SimpleResult[TableDescription]] = {
    val body = Json.obj(
      "TableName" -> tableName,
      "KeySchema" -> keySchema,
      "ProvisionedThroughput" -> provisionedThroughput)
    post[TableDescription]("CreateTable", body)
  }

  def deleteTable(tableName: String)(implicit region: AWSRegion): Future[EmptySimpleResult] = {
    val body = Json.obj("TableName" -> tableName)
    post[Unit]("DeleteTable", body)
  }

  def describeTable(tableName: String)(implicit region: AWSRegion): Future[SimpleResult[TableDescription]] = {
    val body = Json.obj("TableName" -> JsString(tableName))
    post[TableDescription]("DescribeTable", body)
  }

  // TODO: Implement the "Expected" parameter
  def putItem(tableName: String,
              item: Map[String, DDBAttribute],
              returnValues: ReturnValues = ReturnValues.NONE)(implicit region: AWSRegion): Future[SimpleResult[ItemResponse]] = {
    val body = Json.obj(
      "TableName" -> JsString(tableName),
      "Item" -> Json.toJson(item),
      "ReturnValues" -> JsString(returnValues.toString))
    post[ItemResponse]("PutItem", body)
  }

  def deleteItem(tableName: String,
                 key: Key,
                 returnValues: ReturnValues = ReturnValues.NONE)(implicit region: AWSRegion): Future[SimpleResult[ItemResponse]] = {
    val body = Json.obj(
      "TableName" -> JsString(tableName),
      "Key" -> Json.toJson(key),
      "ReturnValues" -> JsString(returnValues.toString))
    post[ItemResponse]("DeleteItem", body)
  }

  // GetItem

  // Query

  // Scan

  // UpdateItem

  // UpdateTable

  // BatchGetItem

  // BatchWriteItem

}

