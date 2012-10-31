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

  implicit def createQueueResultParser = Parser[CreateQueueResult] { r: Response =>
    Success(CreateQueueResult(r.xml \\ "QueueUrl" text))
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SQSMeta, T]] =
    Parser.xmlErrorParser[SQSMeta].or(Parser.resultParser(snsMetaParser, p))

}
