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
 * Represents the status of a table, and whether it is ready for reading or not.
 */
sealed trait Status {
  def status: String
  override def toString = status
}

object Status {
  object CREATING extends Status { override def status = "Creating" }
  object ACTIVE extends Status { override def status = "Active" }
  object DELETING extends Status { override def status = "Deleting" }
  object UPDATING extends Status { override def status = "Updating" }
  def apply(s: String) = s.toLowerCase match {
    case "creating" => CREATING
    case "active" => ACTIVE
    case "deleting" => DELETING
    case "updating" => UPDATING
    case _ => sys.error("Invalid table status: " + s)
  }
}
