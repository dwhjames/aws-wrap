package aws.dynamodb

import play.api.libs.json.Json.toJson
import play.api.libs.json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

package object models {

  // JSON Formatters

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

