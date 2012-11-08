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

package aws.simpledb

import java.util.Date

/**
 * An attribute in SimpleDB
 *
 * @param name
 * @param value
 * @param replace Makes sense only in [[SimpleDB.putAttributes]] or [[SimpleDB.batchPutAttributes]].
 * See the documentation of these functions for details.
 */
case class SDBAttribute(name: String, value: String, replace: Boolean = false)

/**
 * An item as stored in SimpleDB
 */
case class SDBItem(name: String, attributes: Seq[SDBAttribute])

/**
 * A SimpleDB domain, similar to tables that contain similar data. Queries can not be executed across domains.
 */
case class SDBDomain(name: String)

/**
 * Used in [[SimpleDB.deleteAttributes]] to add a condition check to attributes to delete.
 *
 * If value is None, only existence of the attribute is checked. If value is defined, not only
 * the existence but equality to the value is also checked.
 */
case class SDBExpected(name: String, value: Option[String] = None)

/**
 * Metadata about a domain provided by a call to [[SimpleDB.domainMetadata]].
 *
 * @param timestamp The data and time when metadata was calculated
 * @param itemCount The number of all items in the domain
 * @param attributeValueCount The number of all attribute name/value pairs in the domain
 * @param attributeNameCount The number of unique attribute names in the domain
 * @param itemNamesSizeBytes The total size of all item names in the domain, in bytes
 * @param attributeValuesSizeBytes The total size of all attribute values, in bytes
 * @param attributeNamesSizeBytes The total size of all unique attribute names, in bytes
 */
case class SDBDomainMetadata(
  timestamp: Date,
  itemCount: Long,
  attributeValueCount: Long,
  attributeNameCount: Long,
  itemNamesSizeBytes: Long,
  attributeValuesSizeBytes: Long,
  attributeNamesSizeBytes: Long)
