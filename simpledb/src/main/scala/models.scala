package aws.simpledb.models

import java.util.Date

/**
 * An attribute in SimpleDB
 *
 * @param name
 * @param value
 * @param replace Makes sense only in putAttributes or putBatchAttributes. If 
 */
case class SDBAttribute(name: String, value: String, replace: Boolean = false)

/**
 * An item as stored in SimpleDB
 */
case class SDBItem(name: String, attributes: Seq[SDBAttribute])

case class SDBDomain(name: String)

case class SDBExpected(name: String, value: Option[String] = None)

case class SDBDomainMetadata(
  timestamp: Date,
  itemCount: Long,
  attributeValueCount: Long,
  attributeNameCount: Long,
  itemNamesSizeBytes: Long,
  attributeValuesSizeBytes: Long,
  attributeNamesSizeBytes: Long)
