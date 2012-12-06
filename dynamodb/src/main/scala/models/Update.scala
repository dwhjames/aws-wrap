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
