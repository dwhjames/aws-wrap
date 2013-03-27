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

package aws.s3.models

import java.text.SimpleDateFormat
import java.util.Date

import aws.core.parsers.{Parser, Success}

case class Bucket(
  name: String,
  creationDate: Date
)

object Bucket {

  def parseDate(d: String): Date =
    new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'").parse(d)

  implicit def bucketsParser = Parser[Seq[Bucket]] { r =>
    Success((r.xml \\ "Bucket") map { n =>
      Bucket(
        (n \ "Name").text,
        parseDate((n \ "CreationDate").text)
      )
    })
  }
}
