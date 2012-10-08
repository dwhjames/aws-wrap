package aws.core

import scala.util.{ Try, Success, Failure }
import scala.xml.Elem
import play.api.libs.ws.{ Response => WSResponse }

import aws.core.parsers._

case class Metadata(requestId: String, boxUsage: String)
object Metadata {
  def apply(xml: Elem): Metadata = Metadata(xml \\ "RequestId" text, xml \\ "BoxUsage" text)
}

trait Result {
  def metadata: Metadata
}

trait SimpleResult[T] extends Result {
  def body: T
  override def toString = "SimpleResult(%s, %s)".format(metadata, body)
}

object SimpleResult {
  def apply[T](xml: Elem)(implicit extractor: Parser[T]): Try[SimpleResult[T]] = Success(new SimpleResult[T] {
    def metadata = Metadata(xml)
    def body = extractor(xml)
  })

  def unapply[T](s: SimpleResult[T]): Option[(Metadata, T)] = Some((s.metadata, s.body))
}

case class EmptyResult(metadata: Metadata) extends Result

// Would be nice if Error was Exception and SimpleResult
case class Error(res: SimpleResult[Seq[AWSError]]) extends Exception

// TODO: AWS sometimes returns a 200 when there is an error (example: NoSuchDomain error for DomainMetadata)
object EmptyResult {
  def apply(wsresp: WSResponse): Try[EmptyResult] = wsresp.status match {
    case 200 => Success(EmptyResult(Metadata(wsresp.xml)))
    case _ => SimpleResult[Seq[AWSError]](wsresp.xml).flatMap(e => Failure(Error(e)))
  }
}