package aws.dynamodb

import scala.concurrent.Future

import play.api.http._
import play.api.libs.ws._
import play.api.libs.ws.WS._
import play.api.libs.json._

import aws.core._

object DynamoDB {

  import DDBRegion.DEFAULT

  implicit def contentTypeOf_JsValue: ContentTypeOf[JsValue] = {
    ContentTypeOf[JsValue](Some("application/x-amz-json-1.0"))
  }

  val VERSION = "20111205"

  private def request(operation: String, body: JsValue)(implicit region: AWSRegion): Future[Response] = {
    WS.url("https://" + region.host + "/").withHeaders(
      "host" -> region.host,
      "x-amz-date" -> (new java.util.Date()).toString,
      "x-amz-target" -> ("DynamoDB_" + VERSION + "." + operation),
      "Authorization" -> "TODO").post(body)
  }

  def listTables(limit: Option[Int] = None, exclusiveStartTableName: Option[String] = None) = {
    val data = (
      limit.map("Limit" -> Json.toJson(_))
      ++ exclusiveStartTableName.map("ExclusiveStartTableName" -> Json.toJson(_))).toMap
    request("ListTables", Json.toJson(data))
  }

  val ALGO = "AWS4-HMAC-SHA256"

  // This is known as "Signature v4" by Amazon, and could go to core
  private def signature(service: String)(implicit region: AWSRegion) = {
    val stringToSign = ALGO + '\n' +
      AWS.isoBasicFormat(new java.util.Date()) + '\n' +
      (VERSION + "/" + region.subdomain + "/" + service + "/aws4_request") + '\n' // +
    // HexEncode(Hash(CanonicalRequest))
  }

}

