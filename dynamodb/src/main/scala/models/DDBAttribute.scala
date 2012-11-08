/*
 * Copyright 2012 Pellucid and Zenexity
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aws.dynamodb

import aws.core.utils.Crypto

sealed trait DDBAttribute {

  /**
   * Type code as AWS expects it
   */
  def typeCode: String

  /**
   * Tries to convert the DDBAttribute to a T.
   * @return None if the conversion fails, Some(t) if it succeeds
   */
  def asOpt[T](implicit ac: AttributeRead[T]) = ac.convert(this)

  /**
   * Tries to convert the DDBAttribute to a T, throws an error if it fails
   */
  def as[T](implicit ac: AttributeRead[T]) = asOpt(ac).getOrElse(sys.error("Failed conversion"))
}

sealed trait DDBScalar extends DDBAttribute

sealed trait DDBSet extends DDBAttribute

/**
 * This will never be sent to Amazon, this type is only here to ease creation of an item with optional attributes
 */
case object DDBNone extends DDBAttribute {
  def typeCode = "none"
  def asOpt[T] = None
}

object DDBScalar {
  def apply(typeCode: String, value: String): DDBScalar = typeCode match {
    case "N" => DDBNumber(value.toDouble)
    case "S" => DDBString(value)
    case "B" => DDBBinary(Crypto.decodeBase64(value))
    case _ => sys.error("Invalid type code for scalar: " + typeCode)
  }
}

object DDBSet {
  def apply(typeCode: String, value: Set[String]): DDBSet = typeCode match {
    case "NS" => DDBNumberSet(value.map(_.toDouble))
    case "SS" => DDBStringSet(value)
    case "BS" => DDBBinarySet(value.map(Crypto.decodeBase64(_)))
    case _ => sys.error("Invalid type code for set: " + typeCode)
  }
}

object DDBAttribute {
  def apply(typeCode: String, value: String) = DDBScalar(typeCode, value)
  def apply(typeCode: String, value: Set[String]) = DDBSet(typeCode, value)

  def apply(value: String) = DDBString(value)
  def apply(value: Double) = DDBNumber(value)
  def apply(value: Array[Byte]) = DDBBinary(value)
  def apply(value: Iterable[String]) = DDBStringSet(value.toSet)
  def apply(value: Iterable[Double]) = DDBNumberSet(value.toSet)
  def apply(value: Iterable[Array[Byte]]) = DDBBinarySet(value.toSet)

  def write[T](t: T)(implicit aw: AttributeWrite[T]) = aw.writes(t)

}

case class DDBNumber(value: Double) extends DDBScalar {
  def typeCode = "N"
}

case class DDBString(value: String) extends DDBScalar {
  def typeCode = "S"
}

case class DDBBinary(value: Array[Byte]) extends DDBScalar {
  def typeCode = "B"
  override def toString = Crypto.base64(value)
}

case class DDBNumberSet(value: Set[Double]) extends DDBSet {
  def typeCode = "NS"
}

case class DDBStringSet(value: Set[String]) extends DDBSet {
  def typeCode = "SS"
}

case class DDBBinarySet(value: Set[Array[Byte]]) extends DDBSet {
  def typeCode = "BS"
}
