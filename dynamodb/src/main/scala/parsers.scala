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

import play.api.libs.json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.core._
import aws.core.parsers._
import aws.dynamodb.JsonFormats._

object DDBParsers {

  // TODO: Use a type (we may need Seq[String] for something else)
  implicit def tableListParser = Parser[Seq[String]] { r =>
    Success((r.json \ "TableNames").as[Seq[String]])
  }

  implicit def tableDescriptionParser = Parser[TableDescription] { r =>
    ((r.json \ "TableDescription"), (r.json \ "Table")) match {
      case (desc: JsObject, _) => Success(desc.as[TableDescription])
      case (_, desc: JsObject) => Success(desc.as[TableDescription])
      case _ => Failure("Error parsing Json response: " + r.body)
    }
  }

  // Generic parsers

  implicit def jsonResultParser[T](implicit fjs: Reads[T]) = Parser[T] { r =>
    r.json.validate[T] match {
      case JsSuccess(result, _) => Success(result)
      case JsError(errors) => Failure(errors.toString)
    }
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[EmptyMeta.type, T]] =
    jsonErrorsParser.or(Parser.resultParser(Parser.emptyMetadataParser, p))

  implicit val awsErrorFormat = Reads[AWSError[EmptyMeta.type]](js => {
    (js \ "__type", js \ "Message", js \ "message") match {
      case (JsString(t), JsString(m), _) => JsSuccess(AWSError(EmptyMeta, errorCode(t), m))
      case (JsString(t), _, JsString(m)) => JsSuccess(AWSError(EmptyMeta, errorCode(t), m))
      case _ => JsError("JsObject expected")
    }
  })

  val jsonErrorsParser = Parser[AWSError[EmptyMeta.type]] { r =>
    r.status match {
      case 200 => Failure("Not an error")
      case _ => Success(r.json.as[AWSError[EmptyMeta.type]])
    }
  }

  /**
   * Amazon returns errors such as com.amazonaws.dynamodb.v20111205#ProvisionedThroughputExceededException
   * but per their documentation only the portion after # matters
   */
  private def errorCode(errorType: String): String = errorType.split("#").last

}

