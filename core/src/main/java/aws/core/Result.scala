package aws.core

import scala.util.{Try, Success, Failure}
import scala.xml.Elem
import play.api.libs.ws.{Response => WSResponse}

case class Metadata(requestId: String, boxUsage: String)
object Metadata {
  def apply(xml: Elem): Metadata = Metadata(xml \\ "RequestId" text, xml \\ "BoxUsage" text)
}

trait Result{
  def metadata: Metadata
}

trait SimpleResult[T] extends Result {
  def body: T
  override def toString = "SimpleResult(%s, %s)".format(metadata, body)
}

object SimpleResult {
  def apply[T](xml: Elem, parser: (Elem => T)): Try[SimpleResult[T]] = Success(new SimpleResult[T]{
    def metadata = Metadata(xml)
    def body = parser(xml)
  })
}

case class EmptyResult(metadata: Metadata) extends Result

case class Error(metadata: Metadata, errors: Seq[(String, String)]) extends Exception with Result

object EmptyResult {
  def apply(wsresp: WSResponse): Try[EmptyResult] = wsresp.status match {
    case 200 => Success(EmptyResult(Metadata(wsresp.xml)))
    case _ => {
      val errors = wsresp.xml \\ "Error" map { node =>
        (node \ "Code" text) -> (node \ "Message" text)
      }
      Failure(Error(Metadata(wsresp.xml), errors))
    }
  }
}