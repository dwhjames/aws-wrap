package aws.cloudsearch

import java.lang.{ Long => JLong, Double => JDouble, Boolean => JBool }

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit._

import play.api.libs.ws.Response
import aws.core._
import aws.core.parsers._

object CloudSearchParsers {

  implicit def cloudSearchMetadataParser = Parser[CloudSearchMetadata] { r =>
    val i = (r.json \ "hits" \ "info")
    val meta = CloudSearchMetadata(
      (i \ "rid").as[String],
      Duration((i \ "time-ms").as[Long], MILLISECONDS),
      Duration((i \ "cpu-time-ms").as[Long], MILLISECONDS))

    Success(meta)
  }

  implicit def safeResultParser[T](implicit p: Parser[T]): Parser[Result[CloudSearchMetadata, T]] =
    Parser.xmlErrorParser[CloudSearchMetadata].or(Parser.resultParser(cloudSearchMetadataParser, p))
}
