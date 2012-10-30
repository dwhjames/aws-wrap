package aws.dynamodb.models

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

/**
 * A Primary key with only a hash key. See [[PrimaryKey]] for more details.
 */
case class HashKey(hashKey: KeySchemaElement) extends PrimaryKey

/**
 * A Primary key with both a hash key and a range key. See [[PrimaryKey]] for more details.
 */
case class CompositeKey(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) extends PrimaryKey

object PrimaryKey {

  def apply(hashKey: KeySchemaElement) = HashKey(hashKey)

  def apply(hashKey: KeySchemaElement, rangeKey: KeySchemaElement) = CompositeKey(hashKey, rangeKey)

}
