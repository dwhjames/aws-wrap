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

import scala.concurrent.duration._

import aws.core.parsers.{Parser, Success}

case class LifecycleConf(
  id:       Option[String],
  prefix:   String,
  status:   LifecycleStatus.Value,
  lifetime: Duration
)

object LifecycleConf {

  implicit def lifecyclesParser = Parser[Seq[LifecycleConf]] { r =>
    Success((r.xml \ "Rule") map { l =>
      LifecycleConf(
        id       = (l \ "ID").map(_.text).headOption,
        prefix   = (l \ "Prefix").text,
        status   = (l \ "Status").map(n => LifecycleStatus.withName(n.text)).headOption.get,
        lifetime = (l \ "Expiration" \ "Days").map(v => Duration(java.lang.Integer.parseInt(v.text), DAYS)).headOption.get
      )
    })
  }
}
