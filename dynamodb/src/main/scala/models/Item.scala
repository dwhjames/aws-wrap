package aws.dynamodb

case class Item(attributes: Seq[(String, DDBAttribute)]) {

  def keys: Set[String] = attributes.map(_._1).toSet

  def toMap: Map[String, DDBAttribute] = attributes.toMap

  /**
   * Merge this item with an other one, returning a new Item.
   * Values from other override value of the current object.
   */
  def ++(other: Item): Item =
    Item(attributes.filterNot(attr => other.keys(attr._1)) ++ other.attributes)

  /**
   * removes one field from the Item, returning a new Item
   */
  def -(otherField: String): Item = Item(attributes.filterNot(_._1 == otherField))

  /**
   * adds one field to Item, returning a new Item
   */
  def +(otherField: (String, DDBAttribute)): Item = Item(attributes :+ otherField)

  /**
   * Optionally return the attribute corresponding to the key
   */
  def get(key: String): Option[DDBAttribute] = attributes.find(attr => attr._1 == key).map(_._2)

  /**
   * Return the attribute corresponding to the key, throws NoSuchElementException if the attribute doesn't exist
   */
  def appy(key: String): DDBAttribute = get(key) match {
    case Some(attr) => attr
    case None => throw new NoSuchElementException()
  }

}

object Item {

  val empty = Item(Nil)

  def build(attrs: (String, DDBAttribute)*) = new Item(attrs.toSeq)

  def randomUUID: String = java.util.UUID.randomUUID().toString

}

