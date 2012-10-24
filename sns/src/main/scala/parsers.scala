package aws.sns

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object SNSParsers {
  import scala.xml.Elem
  import language.postfixOps

  implicit def snsMetaParser = Parser[SNSMeta] { r =>
    Success(SNSMeta(r.xml \\ "RequestId" text))
  }

  implicit def listTopicsResultParser = Parser[ListTopicsResult] { r: Response =>
    Success(ListTopicsResult(
      (r.xml \\ "TopicArn").map(_.text),
      (r.xml \\ "NextToken").headOption.map(_.text))
    )
  }

  implicit def createTopicsResultParser = Parser[CreateTopicResult] { r: Response =>
    Success(
      CreateTopicResult((r.xml \\ "TopicArn").text)
    )
  }

  implicit def subscribeResultParser = Parser[SubscribeResult] { r: Response =>
    Success(
      SubscribeResult((r.xml \\ "SubscriptionArn").text)
    )
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[SNSMeta, T]] =
    errorsParser.or(Parser.resultParser(snsMetaParser, p))

  def errorsParser = snsMetaParser.flatMap(meta => Parser[Errors[SNSMeta]] { r =>
    r.status match {
      // TODO: really test content
      case 200 => Failure("Not an error")
      case _ => Success(Errors(meta, (r.xml \\ "Error").map { node =>
        AWSError(node \ "Code" text, node \ "Message" text)
      }))
    }
  })

}
