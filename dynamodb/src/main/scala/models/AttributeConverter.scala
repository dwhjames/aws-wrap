package aws.dynamodb

trait AttributeConverter[T] {
  def convert(attribute: DDBAttribute): Option[T]
}

object AttributeConverter {

  implicit val stringConvert = new AttributeConverter[String] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBString(s) => Some(s)
      case _ => None
    }
  }

  implicit val doubleConvert = new AttributeConverter[Double] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBNumber(n) => Some(n)
      case _ => None
    }
  }

  implicit val longConvert = new AttributeConverter[Long] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBNumber(n) => Some(n.toLong)
      case _ => None
    }
  }

  implicit val binaryConvert = new AttributeConverter[Array[Byte]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBBinary(b) => Some(b)
      case _ => None
    }
  }

  implicit val stringSetConvert = new AttributeConverter[Set[String]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBStringSet(ss) => Some(ss)
      case _ => None
    }
  }

  implicit val doubleSetConvert = new AttributeConverter[Set[Double]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBNumberSet(ns) => Some(ns)
      case _ => None
    }
  }

  implicit val binarySetConvert = new AttributeConverter[Set[Array[Byte]]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBBinarySet(bs) => Some(bs)
      case _ => None
    }
  }

  implicit val stringSeqConvert = new AttributeConverter[Seq[String]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBStringSet(ss) => Some(ss.toSeq)
      case _ => None
    }
  }

  implicit val doubleSeqConvert = new AttributeConverter[Seq[Double]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBNumberSet(ns) => Some(ns.toSeq)
      case _ => None
    }
  }

  implicit val binarySeqConvert = new AttributeConverter[Seq[Array[Byte]]] {
    override def convert(attribute: DDBAttribute) = attribute match {
      case DDBBinarySet(bs) => Some(bs.toSeq)
      case _ => None
    }
  }

}
