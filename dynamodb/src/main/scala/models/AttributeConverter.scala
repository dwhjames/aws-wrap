package aws.dynamodb

import scala.annotation.implicitNotFound

@implicitNotFound("I don't know how to convert a DDBAttribute to a ${To}. Provide an implicit AttributeConverter for this type.")
trait AttributeConverter[To] {
  def convert(attribute: DDBAttribute): Option[To]
}

object AttributeConverter {

  def apply[A](f: DDBAttribute => Option[A]): AttributeConverter[A] = new AttributeConverter[A] {
    def convert(attribute: DDBAttribute) = f(attribute)
  }

  implicit val stringConvert = AttributeConverter[String](_ match {
    case DDBString(s) => Some(s)
    case _ => None
  })

  implicit val doubleConvert = AttributeConverter[Double](_ match {
    case DDBNumber(n) => Some(n)
    case _ => None
  })

  implicit val longConvert = AttributeConverter[Long](_ match {
    case DDBNumber(n) => Some(n.toLong)
    case _ => None
  })

  implicit val intConvert = AttributeConverter[Int](_ match {
    case DDBNumber(n) => Some(n.toInt)
    case _ => None
  })

  implicit val binaryConvert = AttributeConverter[Array[Byte]](_ match {
    case DDBBinary(b) => Some(b)
    case _ => None
  })

  implicit val stringSetConvert = AttributeConverter[Set[String]](_ match {
    case DDBStringSet(ns) => Some(ns)
    case _ => None
  })

  implicit val doubleSetConvert = AttributeConverter[Set[Double]](_ match {
    case DDBNumberSet(ns) => Some(ns)
    case _ => None
  })

  implicit val binarySetConvert = AttributeConverter[Set[Array[Byte]]](_ match {
    case DDBBinarySet(bs) => Some(bs)
    case _ => None
  })

  implicit val stringSeqConvert = AttributeConverter[Seq[String]](_ match {
    case DDBStringSet(ss) => Some(ss.toSeq)
    case _ => None
  })

  implicit val doubleSeqConvert = AttributeConverter[Seq[Double]](_ match {
    case DDBNumberSet(ns) => Some(ns.toSeq)
    case _ => None
  })

  implicit val binarySeqConvert = AttributeConverter[Seq[Array[Byte]]](_ match {
    case DDBBinarySet(bs) => Some(bs.toSeq)
    case _ => None
  })

}
