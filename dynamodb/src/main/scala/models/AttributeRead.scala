package aws.dynamodb

import scala.annotation.implicitNotFound

@implicitNotFound("I don't know how to convert a DDBAttribute to a ${To}. Provide an implicit AttributeRead for this type.")
trait AttributeRead[To] {
  def convert(attribute: DDBAttribute): Option[To]
}

object AttributeRead {

  def apply[A](f: DDBAttribute => Option[A]): AttributeRead[A] = new AttributeRead[A] {
    def convert(attribute: DDBAttribute) = f(attribute)
  }

  implicit val stringConvert = AttributeRead[String](_ match {
    case DDBString(s) => Some(s)
    case _ => None
  })

  implicit val doubleConvert = AttributeRead[Double](_ match {
    case DDBNumber(n) => Some(n)
    case _ => None
  })

  implicit val longConvert = AttributeRead[Long](_ match {
    case DDBNumber(n) => Some(n.toLong)
    case _ => None
  })

  implicit val intConvert = AttributeRead[Int](_ match {
    case DDBNumber(n) => Some(n.toInt)
    case _ => None
  })

  implicit val binaryConvert = AttributeRead[Array[Byte]](_ match {
    case DDBBinary(b) => Some(b)
    case _ => None
  })

  implicit val stringSetConvert = AttributeRead[Set[String]](_ match {
    case DDBStringSet(ns) => Some(ns)
    case _ => None
  })

  implicit val doubleSetConvert = AttributeRead[Set[Double]](_ match {
    case DDBNumberSet(ns) => Some(ns)
    case _ => None
  })

  implicit val binarySetConvert = AttributeRead[Set[Array[Byte]]](_ match {
    case DDBBinarySet(bs) => Some(bs)
    case _ => None
  })

  implicit val stringSeqConvert = AttributeRead[Seq[String]](_ match {
    case DDBStringSet(ss) => Some(ss.toSeq)
    case _ => None
  })

  implicit val doubleSeqConvert = AttributeRead[Seq[Double]](_ match {
    case DDBNumberSet(ns) => Some(ns.toSeq)
    case _ => None
  })

  implicit val binarySeqConvert = AttributeRead[Seq[Array[Byte]]](_ match {
    case DDBBinarySet(bs) => Some(bs.toSeq)
    case _ => None
  })

}
