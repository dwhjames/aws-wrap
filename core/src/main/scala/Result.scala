package aws.core

import scala.util.{ Try, Success, Failure }
import scala.xml.Elem
import play.api.libs.ws.{ Response => WSResponse }

import aws.core.parsers._
import aws.core.parsers.Parser._

case class Metadata(requestId: String, boxUsage: String)

trait Result {
  def metadata: Metadata
}

trait SimpleResult[T] extends Result {
  def body: T
  override def toString = "SimpleResult(%s, %s)".format(metadata, body)
}

object SimpleResult {
  def apply[T](r: WSResponse)(implicit parser: Parser[T], metadataParser: Parser[Metadata]): Try[SimpleResult[T]] = HandleError { r: WSResponse =>
    for(
      m <- metadataParser(r);
      b <- parser(r)
    ) yield new SimpleResult[T] {
      def metadata = m
      def body = b
    }
  }(r)

  def unapply[T](s: SimpleResult[T]): Option[(Metadata, T)] = Some((s.metadata, s.body))
}

case class EmptyResult(metadata: Metadata) extends Result
// TODO: AWS sometimes returns a 200 when there is an error (example: NoSuchDomain error for DomainMetadata)
object EmptyResult{
  def apply(r: WSResponse)(implicit parser: Parser[Metadata]): Try[EmptyResult] = parser(r).map(m => new EmptyResult(m))
}
