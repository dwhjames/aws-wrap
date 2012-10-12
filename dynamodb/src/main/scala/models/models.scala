package aws.dynamodb.models

sealed trait DDBAttribute {
  def typeCode: String
}

// TODO: Check the serialization algorithm with AWS
object DDBAttribute {
  def apply(typeCode: String, value: String): DDBAttribute = typeCode match {
    case "N" => DDBNumber(value.toLong)
    case "S" => DDBString(value)
    case "B" => DDBBinary(value.getBytes())
  }
}

case class DDBNumber(value: Long) extends DDBAttribute {
  def typeCode = "N"
}

case class DDBString(value: String) extends DDBAttribute {
  def typeCode = "S"
}

case class DDBBinary(value: Array[Byte]) extends DDBAttribute {
  def typeCode = "B"
}

sealed trait AttributeType {
  def typeCode: String
  override def toString = typeCode
}

case class Item(attributes: Map[String, DDBAttribute])

object AttributeType {
  def apply(t: String) = t.toLowerCase match {
    case "n" => DDBLong
    case "s" => DDBString
    case "b" => DDBBinary
    case _ => sys.error("Invalid AttributeType: " + t)
  }
}

object DDBLong extends AttributeType {
  override def typeCode = "N"
}

object DDBString extends AttributeType {
  override def typeCode = "S"
}

object DDBBinary extends AttributeType {
  override def typeCode = "B"
}

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

case class Key(hashKeyElement: DDBAttribute, rangeKeyElement: Option[DDBAttribute] = None)

case class ProvisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long)

case class TableDescription(name: String,
                            status: Status,
                            creationDateTime: java.util.Date,
                            keySchema: KeySchema,
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


