package aws.dynamodb

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import play.api.libs.ws._
import play.api.libs.ws.WS._
import play.api.libs.json._

import aws.core._
import aws.core.utils._
import aws.core.signature._

import aws.dynamodb.models._

object DynamoDB {

  import aws.dynamodb._

  import DDBRegion.DEFAULT

  private def request(operation: String,
                      body: JsValue)(implicit region: AWSRegion): Future[DDBResult] = {
    val requestTime = new java.util.Date()
    val headers = Seq(
      "host" -> region.host,
      "x-amz-date" -> AWS.isoDateFormat(requestTime),
      "x-amz-target" -> ("DynamoDB_" + V4.VERSION + "." + operation),
      "Content-Type" -> "application/x-amz-json-1.0")
    val allHeaders = headers ++ Seq(
      "Authorization" -> V4.authorizationHeader(requestTime, headers, Nil, body.toString))

    WS.url("https://" + region.host + "/").withHeaders(allHeaders: _*).post(body.toString).map { r =>
      r.status match {
        case 200 => EmptyDDBResult
        case _ => sys.error("Boo")
      }
    }
  }

  private def request[T](operation: String,
                         body: JsValue,
                         jsonPart: JsValue => JsValue = identity)(implicit region: AWSRegion, r: Reads[T]): Future[SimpleDDBResult[T]] = {
    val requestTime = new java.util.Date()
    val headers = Seq(
      "host" -> region.host,
      "x-amz-date" -> AWS.isoDateFormat(requestTime),
      "x-amz-target" -> ("DynamoDB_" + V4.VERSION + "." + operation),
      "Content-Type" -> "application/x-amz-json-1.0")
    val allHeaders = headers ++ Seq(
      "Authorization" -> V4.authorizationHeader(requestTime, headers, Nil, body.toString))

    WS.url("https://" + region.host + "/").withHeaders(allHeaders: _*).post(body.toString).map { result =>
      aws.dynamodb.SimpleDDBResult(jsonPart(result.json))
    }
  }

  def listTables(limit: Option[Int] = None,
                 exclusiveStartTableName: Option[String] = None)(implicit region: AWSRegion): Future[SimpleDDBResult[Seq[String]]] = {
    val data = (
      limit.map("Limit" -> Json.toJson(_))
      ++ exclusiveStartTableName.map("ExclusiveStartTableName" -> Json.toJson(_))).toMap
    request[Seq[String]]("ListTables", Json.toJson(data), _ \ "TableNames")
  }

  def createTable(name: String,
                  keySchema: KeySchema,
                  provisionedThroughput: ProvisionedThroughput)(implicit region: AWSRegion): Future[SimpleDDBResult[TableDescription]] = {
    val body = Json.obj(
      "TableName" -> name,
      "KeySchema" -> keySchema,
      "ProvisionedThroughput" -> provisionedThroughput
    )
    request[TableDescription]("CreateTable", body, _ \ "TableDescription")
  }

  def deleteTable(name: String)(implicit region: AWSRegion) = {
    val body = Json.obj("TableName" -> name)
    request("DeleteTable", body)
  }

  def describeTable(name: String)(implicit region: AWSRegion) = {
    val body = JsObject(Seq("TableName" -> JsString(name)))
    request[TableDescription]("DescribeTable", body, _ \ "Table")
  }

  // DeleteItem

  // GetItem

  // PutItem

  // Query

  // Scan

  // UpdateItem

  // UpdateTable

  // BatchGetItem

  // BatchWriteItem

}

