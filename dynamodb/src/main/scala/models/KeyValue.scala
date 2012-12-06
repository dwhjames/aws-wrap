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
