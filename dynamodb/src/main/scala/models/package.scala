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

package aws.dynamodb

case class ProvisionedThroughput(readCapacityUnits: Long, writeCapacityUnits: Long)

case class TableDescription(name: String,
                            status: Status,
                            creationDateTime: java.util.Date,
                            keySchema: PrimaryKey,
                            provisionedThroughput: ProvisionedThroughput,
                            size: Option[Long])

object ReturnValues extends Enumeration {
  type ReturnValue = Value
  val NONE = Value("NONE")
  val ALL_OLD = Value("ALL_OLD")
}
import ReturnValues.ReturnValue

case class Expected(exists: Option[Boolean] = None, value: Option[DDBAttribute] = None)

case class ItemResponse(item: Item, consumedCapacityUnits: BigDecimal)

case class QueryResponse(
  items: Seq[Item],
  count: Option[Long],
  scannedCount: Option[Long],
  lastEvaluatedKey: Option[KeyValue],
  consumedCapacityUnits: BigDecimal)

