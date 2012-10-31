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
