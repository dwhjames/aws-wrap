package aws.core.parsers

import scala.annotation.implicitNotFound
import scala.xml.Elem

@implicitNotFound(
  "No parser found for type ${T}. Try to implement an implicit aws.core.parsers.Parser for this type.")
trait Parser[T] extends (Elem => T)

case class AWSError(code: String, message: String)

object Parser {

  def apply[A](transformer: (Elem => A)): Parser[A] = new Parser[A] {
    def apply(xml: Elem): A = transformer(xml)
  }

  def of[T](xml: Elem)(implicit extractor: Parser[T]): T = extractor(xml)

  implicit def errorsParser = Parser[Seq[AWSError]] { xml: Elem =>
    (xml \\ "Error").map { node =>
      AWSError(node \ "Code" text, node \ "Message" text)
    }
  }

}
