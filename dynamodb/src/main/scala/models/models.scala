package aws.dynamodb.models

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

sealed trait KeySchemaElement {
  def attribute: String
  def typeCode: String
}

case class StringKey(attribute: String) extends KeySchemaElement {
  val typeCode = "S"
}

case class NumberKey(attribute: String) extends KeySchemaElement {
  val typeCode = "N"
}

case class BinaryKey(attribute: String) extends KeySchemaElement {
  val typeCode = "B"
}

sealed trait PrimaryKey {
  def hashKey: KeySchemaElement
}

case class HashKey(hashKey: KeySchemaElement) extends PrimaryKey

case class CompositeKey(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) extends PrimaryKey

object PrimaryKey {

  def apply(hashKey: KeySchemaElement) = HashKey(hashKey)

  def apply(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) = CompositeKey(hashKey, rangeKey)

}

sealed trait KeyValue {
  def hashKeyElement: DDBAttribute
}

case class HashKeyValue(hashKeyElement: DDBAttribute) extends KeyValue

object HashKeyValue {
  def apply(value: String) = new HashKeyValue(DDBString(value))
  def apply(value: Long) = new HashKeyValue(DDBNumber(value))
  def apply(value: Array[Byte]) = new HashKeyValue(DDBBinary(value))
}

case class CompositeKeyValue(hashKeyElement: DDBAttribute, rangeKeyElement: DDBAttribute) extends KeyValue

object CompositeKeyValue {
  def apply(hash: String, range: String) = new CompositeKeyValue(DDBString(hash), DDBString(range))
  def apply(hash: Long, range: String) = new CompositeKeyValue(DDBNumber(hash), DDBString(range))
  def apply(hash: Array[Byte], range: String) = new CompositeKeyValue(DDBBinary(hash), DDBString(range))

  def apply(hash: String, range: Long) = new CompositeKeyValue(DDBString(hash), DDBNumber(range))
  def apply(hash: Long, range: Long) = new CompositeKeyValue(DDBNumber(hash), DDBNumber(range))
  def apply(hash: Array[Byte], range: Long) = new CompositeKeyValue(DDBBinary(hash), DDBNumber(range))

  def apply(hash: String, range: Array[Byte]) = new CompositeKeyValue(DDBString(hash), DDBBinary(range))
  def apply(hash: Long, range: Array[Byte]) = new CompositeKeyValue(DDBNumber(hash), DDBBinary(range))
  def apply(hash: Array[Byte], range: Array[Byte]) = new CompositeKeyValue(DDBBinary(hash), DDBBinary(range))
}

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
  count: Option[Long],
  scannedCount: Option[Long],
  lastEvaluatedKey: Option[KeyValue],
  consumedCapacityUnits: BigDecimal)

