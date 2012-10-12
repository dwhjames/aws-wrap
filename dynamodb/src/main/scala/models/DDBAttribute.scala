package aws.dynamodb.models

import aws.core.utils.Crypto

sealed trait DDBAttribute {
  def typeCode: String
}

sealed trait DDBScalar extends DDBAttribute

sealed trait DDBSet extends DDBAttribute

object DDBScalar {
  def apply(typeCode: String, value: String): DDBScalar = typeCode match {
    case "N" => DDBNumber(value.toLong)
    case "S" => DDBString(value)
    case "B" => DDBBinary(Crypto.decodeBase64(value))
    case _ => sys.error("Invalid type code for scalar: " + typeCode)
  }
}

object DDBSet {
  def apply(typeCode: String, value: Set[String]): DDBSet = typeCode match {
    case "NS" => DDBNumberSet(value.map(_.toLong))
    case "SS" => DDBStringSet(value)
    case "BS" => DDBBinarySet(value.map(Crypto.decodeBase64(_)))
    case _ => sys.error("Invalid type code for set: " + typeCode)
  }
}

object DDBAttribute {
  def apply(typeCode: String, value: String) = DDBScalar(typeCode, value)
  def apply(typeCode: String, value: Set[String]) = DDBSet(typeCode, value)
}

case class DDBNumber(value: Long) extends DDBScalar {
  def typeCode = "N"
}

case class DDBString(value: String) extends DDBScalar {
  def typeCode = "S"
}

case class DDBBinary(value: Array[Byte]) extends DDBScalar {
  def typeCode = "B"
  override def toString = Crypto.base64(value)
}

case class DDBNumberSet(value: Set[Long]) extends DDBSet {
  def typeCode = "NS"
}

case class DDBStringSet(value: Set[String]) extends DDBSet {
  def typeCode = "SS"
}

case class DDBBinarySet(value: Set[Array[Byte]]) extends DDBSet {
  def typeCode = "BS"
}

sealed trait AttributeType {
  def typeCode: String
  override def toString = typeCode
}

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
