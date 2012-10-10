package aws.core.parsers

import scala.annotation.implicitNotFound

import play.api.libs.ws.Response

import aws.core._

@implicitNotFound(
  "No parser found for type ${To}. Try to implement an implicit aws.core.parsers.Parser for this type."
)
trait Parser[To] extends (Response => ParseResult[To]) {
  def map[B](f: (To => B)): Parser[B] = this.flatMap { parsed => Parser.pure(f(parsed)) }
  def flatMap[B](f: (To => Parser[B])) = Parser[B] { r =>
    this(r).fold[ParseResult[B]](
      e => Failure(e),
      parsed => f(parsed)(r)
    )
  }

  def or[Other >: To, B <: Other](alternative: Parser[B]) = Parser[Other] { r =>
    this(r).fold(
      e => alternative(r).fold(_ => Failure(e), s => Success(s)): ParseResult[Other],
      s => Success(s)
    )
  }
}

sealed trait ParseResult[+To] {
  def fold[U](e: (String => U), s: (To => U)):U
}
case class Success[To](value: To) extends ParseResult[To] {
  override def fold[U](e: (String => U), s: (To => U)):U = s(value)
}
case class Failure(failure: String) extends ParseResult[Nothing] {
  override def fold[U](e: (String => U), s: (Nothing => U)):U = e(failure)
}

object Parser {

  def pure[To](v: To) = Parser[To](_ => Success(v))

  def apply[To](transformer: (Response => ParseResult[To])): Parser[To] = new Parser[To] {
    def apply(r: Response) = transformer(r)
  }

  def parse[To](r: Response)(implicit p: Parser[To]): ParseResult[To] = p(r)

  def resultParser[M <: Metadata, T](implicit mp: Parser[M], p: Parser[T]): Parser[Result[M, T]] =  mp.flatMap { meta =>
    p.map { body =>
      Result(meta, body)
    }
  }

  implicit def unitParser: Parser[Unit] = Parser.pure(())

}
