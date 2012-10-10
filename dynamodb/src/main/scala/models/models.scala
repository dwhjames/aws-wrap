package aws.dynamodb.models

sealed trait DDBAttribute {
  def typeCode: String
}

object DDBAttribute {
  def apply(typeCode: String, value: String) = typeCode match {
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
  def apply(t: String) = t match {
    case "N" => DDBLong
    case "S" => DDBString
    case "B" => DDBBinary
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

sealed trait ReturnValues

object ReturnValues {
  case object NONE extends ReturnValues { override def toString = "NONE" }
  case object ALL_OLD extends ReturnValues { override def toString = "ALL_OLD" }
}

case class PutItemResponse(consumedCapacityUnits: Long, attributes: Map[String, DDBAttribute])

