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

package aws.s3
package models

import java.util.Date

import play.api.libs.json._

import aws.core.parsers.{Parser, Success}

case class Policy(
  id:         Option[String],
  version:    Option[String]  = Some("2008-10-17"),
  statements: Seq[PolicyStatement]
)

object Policy {

  implicit def policyParser = Parser[Policy] { r =>
    Success(r.json.as[Policy])
  }

  implicit val PolicyFormat: Format[Policy] =
    Format[Policy](
      Reads[Policy] { (json: JsValue) =>
        JsSuccess(
          Policy(
            id         = (json \ "Id")       .as[Option[String]],
            version    = (json \ "Version")  .as[Option[String]],
            statements = (json \ "Statement").as[Seq[PolicyStatement]]
          )
        )
      },
      Writes[Policy] { p =>
        Json.obj(
          "Version"   -> p.version,
          "Id"        -> p.id,
          "Statement" -> Json.toJson(p.statements))
      }
    )
}
