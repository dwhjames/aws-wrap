package aws.dynamodb.models

case class Item(attributes: Map[String, DDBAttribute])

sealed trait UpdateAction

object UpdateAction {
  case object PUT extends UpdateAction { override def toString = "PUT" }
  case object DELETE extends UpdateAction { override def toString = "DELETE" }
  case object ADD extends UpdateAction { override def toString = "ADD" }
  def apply(action: String) = action.toLowerCase match {
    case "put" => PUT
    case "delete" => DELETE
    case "add" => ADD
  }
}

case class Update(value: DDBAttribute, action: UpdateAction = UpdateAction.PUT)

case class KeySchemaElement(attributeName: String, attributeType: AttributeType)

sealed trait PrimaryKey

case class HashKey(hashKey: KeySchemaElement) extends PrimaryKey

case class CompositeKey(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) extends PrimaryKey

object PrimaryKey {

  def apply(hashKey: KeySchemaElement) = HashKey(hashKey)

  def apply(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) = CompositeKey(hashKey, rangeKey)

}

case class Key(hashKeyElement: DDBAttribute, rangeKeyElement: Option[DDBAttribute] = None)

case class ProvisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long)

case class TableDescription(name: String,
                            status: Status,
                            creationDateTime: java.util.Date,
                            keySchema: PrimaryKey,
                            provisionedThroughput: ProvisionedThroughput,
                            size: Option[Long])

sealed trait ReturnValues

object ReturnValues {
  case object NONE extends ReturnValues { override def toString = "NONE" }
  case object ALL_OLD extends ReturnValues { override def toString = "ALL_OLD" }
}

case class Expected(exists: Option[Boolean] = None, value: Option[DDBAttribute] = None)

case class ItemResponse(attributes: Map[String, DDBAttribute], consumedCapacityUnits: BigDecimal)

case class QueryResponse(
  items: Seq[Map[String, DDBAttribute]],
  count: Long,
  scannedCount: Option[Long],
  lastEvaluatedKey: Option[Key],
  consumedCapacityUnits: BigDecimal)


