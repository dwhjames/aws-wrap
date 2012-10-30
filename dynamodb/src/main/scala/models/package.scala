package aws.dynamodb

/**
 * Action to be used in [[DynamoDB.updateItem]].
 */
sealed trait Update {
  def value: DDBAttribute
  def action: String
}

object Update {

  case class Put(value: DDBAttribute) extends Update {
    override def action = "PUT"
  }

  case class Delete(value: DDBAttribute) extends Update {
    override def action = "DELETE"
  }

  case class Add(value: DDBAttribute) extends Update {
    override def action = "ADD"
  }

  def put[T](value: T)(implicit wrt: AttributeWrite[T]) = Put(wrt.writes(value))

  def delete[T](value: T)(implicit wrt: AttributeWrite[T]) = Delete(wrt.writes(value))

  def add[T](value: T)(implicit wrt: AttributeWrite[T]) = Add(wrt.writes(value))

  def apply(action: String, value: DDBAttribute) = action.toLowerCase match {
    case "put" => Put(value)
    case "delete" => Delete(value)
    case "add" => Add(value)
    case action => sys.error("Unkown action for Update: " + action)
  }
}

/**
 * An element of a Key schema: can be a hash key or a range key.
 *
 * See [[PrimaryKey]] for more details.
 *
 * @param attribute the name of the attribute holding the key
 * @param typeCode the code corresponding to the type (usually S, N or B) as expected by AWS.
 */
sealed trait KeySchemaElement {
  def attribute: String
  def typeCode: String
}

/**
 * Define a key from a string attribute (see [[DDBString]]).
 *
 * @param attribute the name of the attribute holding the key.
 */
case class StringKey(attribute: String) extends KeySchemaElement {
  val typeCode = "S"
}

/**
 * Define a key from a number attribute (see [[DDBNumber]]).
 *
 * @param attribute the name of the attribute holding the key.
 */
case class NumberKey(attribute: String) extends KeySchemaElement {
  val typeCode = "N"
}

/**
 * Define a key from a binary attribute (see [[DDBBinary]]).
 *
 * @param attribute the name of the attribute holding the key.
 */
case class BinaryKey(attribute: String) extends KeySchemaElement {
  val typeCode = "B"
}

/**
 * When you create a table, in addition to the table name, you must specify the primary key of the table.
 * Amazon DynamoDB supports the following two types of primary keys:
 *
 *  - '''Hash Type Primary Key''' - Implemented by [[HashKey]]. In this case the primary key is made of one attribute, a hash attribute. Amazon DynamoDB builds an unordered hash index on this primary key attribute. In the preceding example, the ProductCatalog has Id as its primary key. It is the hash attribute.
 *  - '''Hash and Range Type Primary Key''' - Implemented by [[CompositeKey]]. In this case, the primary key is made of two attributes. The first attributes is the hash attribute and the second one is the range attribute. Amazon DynamoDB builds an unordered hash index on the hash primary key attribute and a sorted range index on the range primary key attribute. For example, Amazon Web Services maintain several forums (see Discussion Forums). Each forum has many threads of discussion and each thread has many replies. You can potentially model this by creating the following three tables:
 *
 * More details: [[http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey]]
 */
sealed trait PrimaryKey {
  def hashKey: KeySchemaElement
}

case class HashKey(hashKey: KeySchemaElement) extends PrimaryKey

case class CompositeKey(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) extends PrimaryKey

object PrimaryKey {

  def apply(hashKey: KeySchemaElement) = HashKey(hashKey)

  def apply(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) = CompositeKey(hashKey, rangeKey)

}

sealed trait KeyValue {
  def hashKeyElement: DDBAttribute
}

case class HashKeyValue(hashKeyElement: DDBAttribute) extends KeyValue

case class CompositeKeyValue(hashKeyElement: DDBAttribute, rangeKeyElement: DDBAttribute) extends KeyValue

object KeyValue {
  def apply(value: String) = new HashKeyValue(DDBString(value))
  def apply(value: Long) = new HashKeyValue(DDBNumber(value))
  def apply(value: Array[Byte]) = new HashKeyValue(DDBBinary(value))

  def apply(hash: String, range: String) = new CompositeKeyValue(DDBString(hash), DDBString(range))
  def apply(hash: Long, range: String) = new CompositeKeyValue(DDBNumber(hash), DDBString(range))
  def apply(hash: Array[Byte], range: String) = new CompositeKeyValue(DDBBinary(hash), DDBString(range))

  def apply(hash: String, range: Long) = new CompositeKeyValue(DDBString(hash), DDBNumber(range))
  def apply(hash: Long, range: Long) = new CompositeKeyValue(DDBNumber(hash), DDBNumber(range))
  def apply(hash: Array[Byte], range: Long) = new CompositeKeyValue(DDBBinary(hash), DDBNumber(range))

  def apply(hash: String, range: Array[Byte]) = new CompositeKeyValue(DDBString(hash), DDBBinary(range))
  def apply(hash: Long, range: Array[Byte]) = new CompositeKeyValue(DDBNumber(hash), DDBBinary(range))
  def apply(hash: Array[Byte], range: Array[Byte]) = new CompositeKeyValue(DDBBinary(hash), DDBBinary(range))
}

case class ProvisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long)

case class TableDescription(name: String,
                            status: Status,
                            creationDateTime: java.util.Date,
                            keySchema: PrimaryKey,
                            provisionedThroughput: ProvisionedThroughput,
                            size: Option[Long])

sealed trait ReturnValues

object ReturnValues {
  case object NONE extends ReturnValues { override def toString = "NONE" }
  case object ALL_OLD extends ReturnValues { override def toString = "ALL_OLD" }
}

case class Expected(exists: Option[Boolean] = None, value: Option[DDBAttribute] = None)

case class ItemResponse(item: Item, consumedCapacityUnits: BigDecimal)

case class QueryResponse(
  items: Seq[Item],
  count: Option[Long],
  scannedCount: Option[Long],
  lastEvaluatedKey: Option[KeyValue],
  consumedCapacityUnits: BigDecimal)

