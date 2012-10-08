package aws.dynamodb

import scala.concurrent.Future

import play.api.libs.ws._
import play.api.libs.ws.WS._
import play.api.libs.json._

import aws.core._
import aws.core.utils._
import aws.core.signature._

object DynamoDB {

  import DDBRegion.DEFAULT

  private def request(operation: String, body: JsValue)(implicit region: AWSRegion): Future[Response] = {
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

  def listTables(limit: Option[Int] = None, exclusiveStartTableName: Option[String] = None) = {
    val data = (
      limit.map("Limit" -> Json.toJson(_))
      ++ exclusiveStartTableName.map("ExclusiveStartTableName" -> Json.toJson(_))).toMap
    request("ListTables", Json.toJson(data))
  }

}

