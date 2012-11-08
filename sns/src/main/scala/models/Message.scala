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

package aws.sns

import play.api.libs.json._

case class Message(default: String,
                   http: Option[String] = None,
                   https: Option[String] = None,
                   email: Option[String] = None,
                   emailJson: Option[String] = None,
                   sqs: Option[String] = None) {

  def serialize: String = this match {
    case Message(d, None, None, None, None, None) => d
    case Message(d, h, hs, e, ej, s) => Json.obj(
      "default" -> d,
      "http" -> h,
      "https" -> hs,
      "email" -> e,
      "emailJson" -> ej,
      "sqs" -> s).toString
  }

  def json: Boolean = this match {
    case Message(d, None, None, None, None, None) => false
    case _ => true
  }

}

