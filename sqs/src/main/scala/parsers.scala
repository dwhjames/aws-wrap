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

import scala.xml.Node

import play.api.libs.json._
import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

import aws.sqs.SQS.Queue
import aws.sqs.MessageAttributes.MessageAttribute

object SQSParsers {
  import language.postfixOps

  implicit def snsMetaParser = Parser[SQSMeta] { r =>
    Success(SQSMeta(r.xml \\ "RequestId" text))
  }

  implicit def queuesListParser = Parser[Seq[Queue]] { r: Response =>
    Success((r.xml \\ "QueueUrl").map(_.text).map(Queue(_)))
  }

  implicit def queueUrlParser = Parser[Queue] { r: Response =>
    Success(Queue(r.xml \\ "QueueUrl" text))
  }

  implicit def sendMessageParser = Parser[SendMessageResult] { r: Response =>
    Success(SendMessageResult(r.xml \\ "MessageId" text, r.xml \\ "MD5OfMessageBody" text))
  }

  implicit def messageReceiveParser = Parser[Seq[MessageReceive]] { r: Response =>
    Success(r.xml \\ "Message" map { n: Node =>
      MessageReceive(
        n \\ "MessageId" text,
        n \\ "MD5OfBody" text,
        n \\ "ReceiptHandle" text,
        (n \\ "Attribute" map { attrNode: Node =>
          MessageAttributes.withName(attrNode \\ "Name" text) -> (attrNode \\ "Value" text)
        }).toMap
      )
    })
  }

  implicit def batchSendMessageBatchParser = Parser[Seq[MessageResponse]] { r: Response =>
    Success(r.xml \\ "SendMessageBatchResultEntry" map { n: Node =>
      MessageResponse(n \\ "Id" text, n \\ "MessageId" text, n \\ "MD5OfMessageBody" text)
    })
  }

  implicit def batchMessageIdParser = Parser[Seq[String]] { r: Response =>
    Success(r.xml \\ "Id" map(_.text))
  }

  implicit def attributeListParser = Parser[Seq[QueueAttribute]] { r: Response =>
    Success(r.xml \\ "Attribute" map { n: Node => 
      QueueAttribute(n \\ "Name" text, n \\ "Value" text)
    })
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SQSMeta, T]] =
    Parser.xmlErrorParser[SQSMeta].or(Parser.resultParser(snsMetaParser, p))

}
