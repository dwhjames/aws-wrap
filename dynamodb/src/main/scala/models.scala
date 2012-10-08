package aws.dynamodb

import play.api.libs.json._
import play.api.libs.json.Json.toJson

package object models {

  sealed trait AttributeType {
    def typeCode: String
    override def toString = typeCode
  }

  object AttributeType {
    def apply(t: String) = t match {
      case "N" => DDBLong
      case "S" => DDBString
      case "B" => DDBBoolean
      case _ => sys.error("Invalid AttributeType: " + t)
    }
  }

  object DDBLong extends AttributeType {
    override def typeCode = "N"
  }

  object DDBString extends AttributeType {
    override def typeCode = "S"
  }

  object DDBBoolean extends AttributeType {
    override def typeCode = "B"
  }

  sealed trait Status {
    def status: String
  }

  object Status {
    object CREATING extends Status { override def status = "Creating" }
    object ACTIVE extends Status { override def status = "Active" }
    object DELETING extends Status { override def status = "Deleting" }
    object UPDATING extends Status { override def status = "Updating" }
    def apply(s: String) = s.toLowerCase match {
      case "creating" => CREATING
      case "active" => ACTIVE
      case "deleting" => DELETING
      case "updating" => UPDATING
      case _ => sys.error("Invalid table status: " + s)
    }
  }

  case class KeySchemaElement(attributeName: String, attributeType: AttributeType)

  case class KeySchema(hashKey: KeySchemaElement, rangeKey: Option[KeySchemaElement] = None)

  case class ProvisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long)

  case class TableDescription(name: String,
                              status: Status,
                              creationDateTime: java.util.Date,
                              keySchema: KeySchema,
                              provisionedThroughput: ProvisionedThroughput,
                              size: Option[Long])

  // JSON Formatters

  implicit def KeySchemaElementFormat: Format[KeySchemaElement] = new Format[KeySchemaElement] {
    def writes(element: KeySchemaElement) = JsObject(Seq(
      "AttributeName" -> JsString(element.attributeName),
      "AttributeType" -> JsString(element.attributeType.toString)))
    def reads(json: JsValue) = JsSuccess(KeySchemaElement(
      (json \ "AttributeName").as[String],
      AttributeType((json \ "AttributeType").as[String])))
  }

  implicit def AttributeTypeFormat: Format[AttributeType] = new Format[AttributeType] {
    def writes(t: AttributeType) = JsString(t.typeCode)
    def reads(json: JsValue) = json match {
      case JsString(t) => JsSuccess(AttributeType(t))
      case _ => JsError("String expected")
    }
  }

  implicit def KeySchemaFormat: Format[KeySchema] = new Format[KeySchema] {
    def writes(schema: KeySchema) = JsObject(
      Seq("HashKeyElement" -> toJson(schema.hashKey)) ++ schema.rangeKey.toSeq.map("rangeKeyElement" -> toJson(_)))
    def reads(json: JsValue) = JsSuccess(KeySchema(
      (json \ "HashKeyElement").as[KeySchemaElement],
      (json \ "RangeKeyElement").asOpt[KeySchemaElement]))
  }

  implicit def ProvisionedThroughputFormat = new Format[ProvisionedThroughput] {
    def writes(pt: ProvisionedThroughput) = JsObject(Seq(
      "ReadCapacityUnits" -> Json.toJson(pt.readCapacityUnits),
      "WriteCapacityUnits" -> Json.toJson(pt.writeCapacityUnits)))
    def reads(json: JsValue) = JsSuccess(ProvisionedThroughput(
      (json \ "ReadCapacityUnits").as[Long],
      (json \ "WriteCapacityUnits").as[Long]))
  }

  implicit def TableDescriptionFormat = new Format[TableDescription] {
    def writes(desc: TableDescription) = JsObject(Seq(
      "TableName" -> Json.toJson(desc.name),
      "KeySchema" -> Json.toJson(desc.keySchema),
      "ProvisionedThroughput" -> Json.toJson(desc.provisionedThroughput),
      "TableSizeBytes" -> Json.toJson(desc.size),
      "TableStatus" -> Json.toJson(desc.status.status)))
    def reads(json: JsValue) = JsSuccess(TableDescription(
      (json \ "TableName").as[String],
      Status((json \ "TableStatus").as[String]),
      new java.util.Date((json \ "CreationDateTime").as[Float].toLong),
      (json \ "KeySchema").as[KeySchema],
      (json \ "ProvisionedThroughput").as[ProvisionedThroughput],
      (json \ "TableSizeBytes").asOpt[Long]))
  }

}

