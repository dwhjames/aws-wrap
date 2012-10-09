package aws.dynamodb

import play.api.libs.json.Json.toJson
import play.api.libs.json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

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
    override def toString = status
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

  /*implicit def KeySchemaElementFormat: Format[KeySchemaElement] = new Format[KeySchemaElement] {
    def writes(element: KeySchemaElement) = JsObject(Seq(
      "AttributeName" -> JsString(element.attributeName),
      "AttributeType" -> JsString(element.attributeType.toString)))
    def reads(json: JsValue) = JsSuccess(KeySchemaElement(
      (json \ "AttributeName").as[String],
      AttributeType((json \ "AttributeType").as[String])))
  }*/

  /*implicit def AttributeTypeFormat: Format[AttributeType] = new Format[AttributeType] {
    def writes(t: AttributeType) = JsString(t.typeCode)
    def reads(json: JsValue) = json match {
      case JsString(t) => JsSuccess(AttributeType(t))
      case _ => JsError("String expected")
    }
  }*/


  def optionalFormat[A](path:JsPath)(implicit reads:Reads[A], writes: Writes[Option[A]]): OFormat[Option[A]] = 
    OFormat(Reads.optional(path)(reads), Writes.optional(path)(writes))

  // should be written in a nicer and more symmetric way in more recent code not yet in master ;)
  implicit val AttributeTypeFormat = Format[AttributeType](
    __.read[String].map(t => AttributeType(t)),
    Writes((at: AttributeType) => JsString(at.typeCode)))

  implicit val StatusFormat = Format[Status](
    __.read[String].map(s => Status(s)),
    Writes((s: Status) => JsString(s.status)))

  implicit val KeySchemaElementFormat = (
    (__ \ 'AttributeName).format[String] and
    (__ \ 'AttributeType).format[AttributeType])(KeySchemaElement, unlift(KeySchemaElement.unapply))

  implicit val KeySchemaFormat = (
    (__ \ 'HashKeyElement).format[KeySchemaElement] and
    optionalFormat[KeySchemaElement]( __ \ 'RangeKeyElement )
  )(KeySchema, unlift(KeySchema.unapply))

  implicit val ProvisionedThroughputFormat = (
    (__ \ 'ReadCapacityUnits).format[Long] and
    (__ \ 'WriteCapacityUnits).format[Long])(ProvisionedThroughput, unlift(ProvisionedThroughput.unapply))

  implicit val TableDescriptionFormat = (
    (__ \ 'TableName).format[String] and
    (__ \ 'TableStatus).format[Status] and
    (__ \ 'CreationDateTime).format[java.util.Date] and
    (__ \ 'KeySchema).format[KeySchema] and
    (__ \ 'ProvisionedThroughput).format[ProvisionedThroughput] and
    optionalFormat[Long](__ \ 'TableSizeBytes))(TableDescription, unlift(TableDescription.unapply))

}

