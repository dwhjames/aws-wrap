package aws.dynamodb

import play.api.libs.json._
import play.api.libs.json.Json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

package object models {

  // JSON Formatters

  def optionalFormat[A](path: JsPath)(implicit reads: Reads[A], writes: Writes[Option[A]]): OFormat[Option[A]] =
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

  implicit val HashKeyFormat = Format[HashKey](
    (__ \ 'HashKeyElement).read[KeySchemaElement].map(e => HashKey(e)),
    Writes((key: HashKey) => Json.obj("HashKeyElement" -> Json.toJson(key.hashKey)))
  )

  implicit val CompositeKeyFormat = (
    (__ \ 'HashKeyElement).format[KeySchemaElement] and
    (__ \ 'RangeKeyElement).format[KeySchemaElement]
  )(CompositeKey, unlift(CompositeKey.unapply))

  implicit val PrimaryKeyFormat = Format[PrimaryKey](
    Reads((json: JsValue) => ((json \ "HashKeyElement").validate[KeySchemaElement], (json \ "RangeKeyElement").asOpt[KeySchemaElement]) match {
      case (JsSuccess(hashKeyElt, _), None) => JsSuccess(HashKey(hashKeyElt))
      case (JsSuccess(hashKeyElt, _), Some(rangeKeyElt)) => JsSuccess(CompositeKey(hashKeyElt, rangeKeyElt))
      case (err: JsError, _) => err
    }),
    Writes((key: PrimaryKey) => key match {
      case h:HashKey => Json.toJson(h)
      case c:CompositeKey => Json.toJson(c)
    })
  )

  implicit val ProvisionedThroughputFormat = (
    (__ \ 'ReadCapacityUnits).format[Long] and
    (__ \ 'WriteCapacityUnits).format[Long])(ProvisionedThroughput, unlift(ProvisionedThroughput.unapply))

  implicit val TableDescriptionFormat = (
    (__ \ 'TableName).format[String] and
    (__ \ 'TableStatus).format[Status] and
    (__ \ 'CreationDateTime).format[java.util.Date] and
    (__ \ 'KeySchema).format[PrimaryKey] and
    (__ \ 'ProvisionedThroughput).format[ProvisionedThroughput] and
    optionalFormat[Long](__ \ 'TableSizeBytes))(TableDescription, unlift(TableDescription.unapply))

  implicit val DDBAttributeFormat = Format[DDBAttribute](
    Reads((json: JsValue) => json match {
      case JsObject(o) if o.size > 0 => JsSuccess(DDBAttribute.apply(o.head._1, o.head._2.as[String]))
      case _ => JsError("Expecting a non empty JsObject")
    }),
    Writes((a: DDBAttribute) => a match {
      case DDBString(s) => Json.obj(a.typeCode -> JsString(s))
      case DDBNumber(n) => Json.obj(a.typeCode -> JsString(n.toString)) // This looks wrong, but AWS actually wants numbers as strings
      case DDBBinary(b) => Json.obj(a.typeCode -> JsString(b.toString))
    }))

  implicit val ItemResponseReads = Reads[ItemResponse](json => {
    (json \ "ConsumedCapacityUnits", json \ "Item") match {
      case (JsNumber(consumed), JsObject(o)) => JsSuccess(
        ItemResponse(o.toMap.mapValues(_.as[DDBAttribute]), consumed))
      case (JsNumber(consumed), _) => JsSuccess(ItemResponse(Map.empty, consumed))
      case _ => JsError("ConsumedCapacityUnits is required and must be a number")
    }
  })

  implicit val KeyFormat = Format[Key](
    Reads((json: JsValue) =>
      ((json \ "HashKeyElement").validate[DDBAttribute], (json \ "RangeKeyElement").asOpt[DDBAttribute]) match {
        case (err: JsError, _) => err
        case (JsSuccess(hashKey, _), rangeKeyOpt) => JsSuccess(Key(hashKey, rangeKeyOpt))
      }),
    Writes((key: Key) => Json.obj("HashKeyElement" -> key.hashKeyElement) ++
      key.rangeKeyElement.map(range => Json.obj("RangeKeyElement" -> range)).getOrElse(Json.obj())))

  implicit val ExpectedWrites = Writes[Expected](expected => {
    val existsJs = expected.exists.map(ex => Json.obj("Exists" -> JsBoolean(ex))).getOrElse(Json.obj())
    val valueJs = expected.value.map(v => Json.obj("Exists" -> toJson(v))).getOrElse(Json.obj())
    existsJs ++ valueJs
  })

  implicit val AttributeActionFormat = Format[UpdateAction](
    __.read[String].map(a => UpdateAction(a)),
    Writes((action: UpdateAction) => JsString(action.toString)))

  implicit val UpdateFormat = (
    (__ \ 'Value).format[DDBAttribute] and
    (__ \ 'Action).format[UpdateAction])(Update, unlift(Update.unapply))

  implicit val KeyConditionWrites = Writes[KeyCondition](_ match {
    case KeyCondition.EqualTo(value) => Json.obj(
      "ComparisonOperator" -> JsString("EQ"),
      "AttributeValueList" -> Json.arr(Json.toJson(value)))
    case KeyCondition.LessThan(value, orEqual) => Json.obj(
      "ComparisonOperator" -> JsString(if (orEqual) "LE" else "LT"),
      "AttributeValueList" -> Json.arr(Json.toJson(value)))
    case KeyCondition.GreaterThan(value, orEqual) => Json.obj(
      "ComparisonOperator" -> JsString(if (orEqual) "GE" else "GT"),
      "AttributeValueList" -> Json.arr(Json.toJson(value)))
    case KeyCondition.BeginsWith(value) => Json.obj(
      "ComparisonOperator" -> JsString("BEGIN_WITH"),
      "AttributeValueList" -> Json.arr(Json.toJson(value)))
    case KeyCondition.Between(lowerBound, upperBound) => Json.obj(
      "ComparisonOperator" -> JsString("BETWEEN"),
      "AttributeValueList" -> Json.arr(Json.toJson(lowerBound), Json.toJson(upperBound)))
  })

  implicit val QueryWrites = Writes[Query](query =>
    Json.obj(
      "TableName" -> JsString(query.tableName),
      "HashKeyValue" -> Json.toJson(query.hashKeyValue),
      "ConsistentRead" -> JsBoolean(query.consistentRead),
      "Count" -> JsBoolean(query.count),
      "ScanIndexForward" -> JsBoolean(query.scanIndexForward)) ++ query.limit.map(l => Json.obj("Limit" -> JsNumber(l))).getOrElse(Json.obj()) ++
      (query.attributesToGet match {
        case Nil => Json.obj()
        case attr => Json.obj("AttributesToGet" -> Json.toJson(attr))
      }) ++
      query.rangeKeyCondition.map(cond => Json.obj("RangeKeyCondition" -> Json.toJson(cond))).getOrElse(Json.obj()) ++
      query.exclusiveStartKey.map(start => Json.obj("ExclusiveStartKey" -> Json.toJson(start))).getOrElse(Json.obj()))

  implicit val QueryResponseReads = Reads[QueryResponse](json => {
    val items: Seq[Map[String, DDBAttribute]] = (json \ "Items") match {
      case JsArray(a) => a.map(_.as[Map[String, DDBAttribute]])
      case _ => Nil
    }
    val count = (json \ "Count").as[Long]
    val scannedCount = (json \ "ScannedCount").asOpt[Long]
    val lastEvaluatedKey = (json \ "LastEvaluatedKey").asOpt[Key]
    val consumedCapacityUnits = (json \ "ConsumedCapacityUnits").as[BigDecimal]
    JsSuccess(QueryResponse(items, count, scannedCount, lastEvaluatedKey, consumedCapacityUnits))
  })

}

