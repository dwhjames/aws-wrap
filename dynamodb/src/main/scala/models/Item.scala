package aws.dynamodb

/**
 * An item in DynamoDB.
 *
 * Building an Item:
 * {{{
 * val item = Item.build(
 *   "id" -> "john@example.com",
 *   "name" -> "John Martin",
 *   "age" -> 25,
 *   "groups" -> Set("admin", "developers", "users")
 * )
 * item: aws.dynamodb.Item = Item(ArrayBuffer((id,DDBString(john@example.com)),
 *       (name,DDBString(John Martin)),
 *       (age,DDBNumber(25.0)),
 *       (groups,DDBStringSet(Set(admin, developers, users)))))
 * }}}
 */
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

  import AttributeWrite._
  import language.implicitConversions

  val empty = Item(Nil)

  /**
   * Build a DynamoDB item, using any type where a [[AttributeWrite]] is available.
   *
   * Example:
   * {{{
   * val item = Item.build(
   *   "id" -> "john@example.com",
   *   "name" -> "John Martin",
   *   "age" -> 25,
   *   "groups" -> Set("admin", "developers", "users")
   * )
   * item: aws.dynamodb.Item = Item(ArrayBuffer((id,DDBString(john@example.com)),
   *       (name,DDBString(John Martin)),
   *       (age,DDBNumber(25.0)),
   *       (groups,DDBStringSet(Set(admin, developers, users)))))
   * }}}
   */
  def build(attrs: (String, AttributeWrapper)*) = new Item(
    attrs.toSeq
      .map(pair => (pair._1 -> pair._2.asInstanceOf[AttributeWrapperImpl].attr))
      .filterNot(_._2 == DDBNone))

  def randomUUID: String = java.util.UUID.randomUUID().toString

  // This is what allows to do Item.build nicely

  sealed trait AttributeWrapper

  private case class AttributeWrapperImpl(attr: DDBAttribute) extends AttributeWrapper

  implicit def toAttributeWrapper[T](field: T)(implicit w: AttributeWrite[T]): AttributeWrapper = AttributeWrapperImpl(w.writes(field))

}

