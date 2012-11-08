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

package aws.sqs

import play.api.libs.json._
import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object SQSParsers {
  import language.postfixOps

  implicit def snsMetaParser = Parser[SQSMeta] { r =>
    Success(SQSMeta(r.xml \\ "RequestId" text))
  }

  implicit def queuesListParser = Parser[QueuesList] { r: Response =>
    Success(QueuesList((r.xml \\ "QueueUrl").map(_.text)))
  }

  implicit def queueUrlParser = Parser[String] { r: Response =>
    Success(r.xml \\ "QueueUrl" text)
  }

  implicit def sendMessageParser = Parser[SendMessageResult] { r: Response =>
    Success(SendMessageResult(r.xml \\ "MessageId" text, r.xml \\ "MD5OfMessageBody" text))
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SQSMeta, T]] =
    Parser.xmlErrorParser[SQSMeta].or(Parser.resultParser(snsMetaParser, p))

}
