package aws.dynamodb

import play.api.libs.json._
import play.api.libs.json.Json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.core.utils.Crypto

object JsonFormats {

  // JSON Formatters

  implicit val StatusFormat = Format[Status](
    __.read[String].map(s => Status(s)),
    Writes((s: Status) => JsString(s.status)))

  implicit val KeySchemaElementFormat = Format[KeySchemaElement](
    Reads((json: JsValue) => ((json \ "AttributeType").validate[String], (json \ "AttributeName").validate[String]) match {
      case (JsSuccess("S", _), JsSuccess(name, _)) => JsSuccess(StringKey(name))
      case (JsSuccess("N", _), JsSuccess(name, _)) => JsSuccess(NumberKey(name))
      case (JsSuccess("B", _), JsSuccess(name, _)) => JsSuccess(BinaryKey(name))
      case (JsSuccess(t, _), _) => JsError("Unknown attribute type: " + t)
      case (err: JsError, _) => err
      case (_, err: JsError) => err
    }),
    Writes((key: KeySchemaElement) => Json.obj(
      "AttributeType" -> key.typeCode,
      "AttributeName" -> key.attribute)))

  implicit val HashKeyFormat = Format[HashKey](
    (__ \ 'HashKeyElement).read[KeySchemaElement].map(e => HashKey(e)),
    Writes((key: HashKey) => Json.obj("HashKeyElement" -> Json.toJson(key.hashKey))))

  implicit val CompositeKeyFormat = (
    (__ \ 'HashKeyElement).format[KeySchemaElement] and
    (__ \ 'RangeKeyElement).format[KeySchemaElement])(CompositeKey, unlift(CompositeKey.unapply))

  implicit val PrimaryKeyFormat = Format[PrimaryKey](
    Reads((json: JsValue) => ((json \ "HashKeyElement").validate[KeySchemaElement], (json \ "RangeKeyElement").asOpt[KeySchemaElement]) match {
      case (JsSuccess(hashKeyElt, _), None) => JsSuccess(HashKey(hashKeyElt))
      case (JsSuccess(hashKeyElt, _), Some(rangeKeyElt)) => JsSuccess(CompositeKey(hashKeyElt, rangeKeyElt))
      case (err: JsError, _) => err
    }),
    Writes((key: PrimaryKey) => key match {
      case h: HashKey => Json.toJson(h)
      case c: CompositeKey => Json.toJson(c)
    }))

  implicit val ProvisionedThroughputFormat = (
    (__ \ 'ReadCapacityUnits).format[Long] and
    (__ \ 'WriteCapacityUnits).format[Long])(ProvisionedThroughput, unlift(ProvisionedThroughput.unapply))

  implicit val TableDescriptionFormat = (
    (__ \ 'TableName).format[String] and
    (__ \ 'TableStatus).format[Status] and
    (__ \ 'CreationDateTime).format[java.util.Date] and
    (__ \ 'KeySchema).format[PrimaryKey] and
    (__ \ 'ProvisionedThroughput).format[ProvisionedThroughput] and
    Format.optional[Long](__ \ 'TableSizeBytes))(TableDescription, unlift(TableDescription.unapply))

  implicit val DDBAttributeFormat = Format[DDBAttribute](
    Reads((json: JsValue) => json match {
      case JsObject(o) if o.size > 0 => {
        o.head._1 match {
          case attr if Set("N", "S", "B")(attr) => JsSuccess(DDBAttribute.apply(o.head._1, o.head._2.as[String]))
          case attr if Set("NS", "SS", "BS")(attr) => JsSuccess(DDBAttribute.apply(o.head._1, o.head._2.as[Set[String]]))
          case attr => JsError("Unkown attribute type " + attr)
        }
      }
      case _ => JsError("Expecting a non empty JsObject")
    }),
    Writes((a: DDBAttribute) => a match {
      case DDBString(s) => Json.obj(a.typeCode -> JsString(s))
      case DDBNumber(n) => Json.obj(a.typeCode -> JsString(n.toString)) // This looks wrong, but AWS actually wants numbers as strings
      case DDBBinary(b) => Json.obj(a.typeCode -> JsString(Crypto.base64(b)))
      case DDBStringSet(ss) => Json.obj(a.typeCode -> Json.toJson(ss))
      case DDBNumberSet(ns) => Json.obj(a.typeCode -> Json.toJson(ns))
      case DDBBinarySet(bs) => Json.obj(a.typeCode -> Json.toJson(bs.map(Crypto.base64(_))))
    }))

  implicit val ItemFormat = Format[Item](
    Reads((json: JsValue) => json match {
      case JsObject(o) => JsSuccess(
        Item(o.map(pair => (pair._1 -> pair._2.as[DDBAttribute])))
      )
      case _ => JsError("Expected a JsObject")
    }),
    Writes((item: Item) => Json.toJson(item.attributes.toMap))
  )

  implicit val ItemResponseReads = Reads[ItemResponse](json => {
    ((json \ "ConsumedCapacityUnits").validate[BigDecimal], (json \ "Item").validate[Item]) match {
      case (JsSuccess(consumed, _), JsSuccess(item, _)) => JsSuccess(ItemResponse(item, consumed))
      case (JsSuccess(consumed, _), _) => JsSuccess(ItemResponse(Item.empty, consumed))
      case _ => JsError("ConsumedCapacityUnits is required and must be a number")
    }
  })

  implicit val HashKeyValueFormat = Format[HashKeyValue](
    (__ \ 'HashKeyElement).read[DDBAttribute].map(e => HashKeyValue(e)),
    Writes((key: HashKeyValue) => Json.obj("HashKeyElement" -> Json.toJson(key.hashKeyElement))))

  implicit val CompositeKeyValueFormat = (
    (__ \ 'HashKeyElement).format[DDBAttribute] and
    (__ \ 'RangeKeyElement).format[DDBAttribute])(CompositeKeyValue.apply _, unlift(CompositeKeyValue.unapply))

  implicit val KeyValueFormat = Format[KeyValue](
    Reads((json: JsValue) =>
      ((json \ "HashKeyElement").validate[DDBAttribute], (json \ "RangeKeyElement").validate[DDBAttribute]) match {
        case (err: JsError, _) => err
        case (JsSuccess(hashKey, _), JsSuccess(rangeKey, _)) => JsSuccess(CompositeKeyValue(hashKey, rangeKey))
        case (JsSuccess(hashKey, _), _) => JsSuccess(HashKeyValue(hashKey))
      }),
    Writes((key: KeyValue) => key match {
      case h: HashKeyValue => Json.toJson(h)
      case c: CompositeKeyValue => Json.toJson(c)
    }))

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
    val items: Seq[Item] = (json \ "Items").validate[Seq[Item]] match {
      case JsSuccess(items, _) => items
      case _ => Nil
    }
    val count = (json \ "Count").asOpt[Long]
    val scannedCount = (json \ "ScannedCount").asOpt[Long]
    val lastEvaluatedKey = (json \ "LastEvaluatedKey").asOpt[KeyValue]
    val consumedCapacityUnits = (json \ "ConsumedCapacityUnits").as[BigDecimal]
    JsSuccess(QueryResponse(items, count, scannedCount, lastEvaluatedKey, consumedCapacityUnits))
  })

  implicit val WriteRequestFormat = Format[WriteRequest](
    Reads((json: JsValue) => ((json \ "PutRequest" \ "Item").validate[Map[String, DDBAttribute]],
      (json \ "DeleteRequest" \ "Key").validate[KeyValue]) match {
        case (JsSuccess(items, _), _) => JsSuccess(PutRequest(items))
        case (_, JsSuccess(key, _)) => JsSuccess(DeleteRequest(key))
        case (err: JsError, _) => err
        case (_, err: JsError) => err
      }),
    Writes((request: WriteRequest) => request match {
      case PutRequest(items) => Json.obj("PutRequest" ->
        Json.obj("Item" -> Json.toJson(items.toMap)))
      case DeleteRequest(key) => Json.obj("DeleteRequest" ->
        Json.obj("Key" -> Json.toJson(key)))
    }))

  implicit val BatchGetRequestWrites = Format[GetRequest](
    Reads(json => ((json \ "Keys").validate[Seq[KeyValue]], (json \ "AttributesToGet").validate[Seq[String]]) match {
      case (JsSuccess(keys, _), JsSuccess(attr, _)) => JsSuccess(GetRequest(keys, attr))
      case (JsSuccess(keys, _), _) => JsSuccess(GetRequest(keys))
      case (err: JsError, _) => err
    }),
    Writes(request =>
      Json.obj("Keys" -> Json.toJson(request.keys)) ++
        (request.attributesToGet match {
          case Nil => Json.obj()
          case attr => Json.obj("AttributesToGet" -> Json.toJson(attr))
        })))

  implicit val BatchWriteResponseReads = Reads[BatchWriteResponse](json => {
    val responses = (json \ "Responses").as[Map[String, QueryResponse]].toSeq
    val unprocessed = (json \ "UnprocessedItems").as[Map[String, Seq[WriteRequest]]].toSeq
    JsSuccess(BatchWriteResponse(responses, unprocessed))
  })

  implicit val BatchGetResponseReads = Reads[BatchGetResponse](json => {
    val responses = (json \ "Responses").as[Map[String, QueryResponse]].toSeq
    val unprocessed = (json \ "UnprocessedKeys").as[Map[String, Seq[GetRequest]]].toSeq
    JsSuccess(BatchGetResponse(responses, unprocessed))
  })

}

