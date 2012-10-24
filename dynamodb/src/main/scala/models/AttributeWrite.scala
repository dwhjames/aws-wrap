package aws.dynamodb

import scala.annotation.implicitNotFound

@implicitNotFound("I don't know how to convert a ${T} to a DDBAttribute. Provide an implicit AttributeWrite for this type.")
trait AttributeWrite[-T] {
  def writes(t: T): DDBAttribute
}

object AttributeWrite {

  def apply[A](f: A => DDBAttribute): AttributeWrite[A] = new AttributeWrite[A] {
    def writes(a: A) = f(a)
  }

  implicit val identityWrite = AttributeWrite[DDBAttribute](identity)

  implicit val stringWrite = AttributeWrite[String](DDBString(_))

  implicit val doubleWrite = AttributeWrite[Double](DDBNumber(_))

  implicit val longWrite = AttributeWrite[Long] { n: Long => DDBNumber(n.toDouble) }

  implicit val intWrite = AttributeWrite[Int] { n: Int => DDBNumber(n.toDouble) }

  implicit val binaryWrite = AttributeWrite[Array[Byte]](DDBBinary(_))

  implicit val stringSetWrite = AttributeWrite[Set[String]](DDBStringSet(_))

  implicit val doubleSetWrite = AttributeWrite[Set[Double]](DDBNumberSet(_))

  implicit val binarySetWrite = AttributeWrite[Set[Array[Byte]]](DDBBinarySet(_))

  implicit val stringSeqWrite = AttributeWrite[Iterable[String]] { ss: Iterable[String] => DDBStringSet(ss.toSet) }

  implicit val doubleSeqWrite = AttributeWrite[Iterable[Double]] { ns: Iterable[Double] => DDBNumberSet(ns.toSet) }

  implicit val binarySeqWrite = AttributeWrite[Iterable[Array[Byte]]] { bs: Iterable[Array[Byte]] => DDBBinarySet(bs.toSet) }

  implicit def optionWrite[T](implicit aw: AttributeWrite[T]) = AttributeWrite[Option[T]](_ match {
    case None => DDBNone
    case Some(t) => aw.writes(t)
  })

}
