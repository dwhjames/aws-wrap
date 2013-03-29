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
package aws.core.parsers {

  import scala.annotation.implicitNotFound
  import play.api.libs.ws.Response
  import aws.core._

  @implicitNotFound("No parser found for type ${To}. Try to implement an implicit aws.core.parsers.Parser for this type.")
  trait Parser[To] extends (Response => ParseResult[To]) {
    def map[B](f: (To => B)): Parser[B] = this.flatMap { parsed => Parser.pure(f(parsed)) }
    def flatMap[B](f: (To => Parser[B])) = Parser[B] { r =>
      this(r).fold[ParseResult[B]](
        e => Failure(e),
        parsed => f(parsed)(r))
    }

    def and[B](pb: Parser[B]) = Parser[(To, B)] { r =>
      val r1 = this(r)
      val r2 = pb(r)
      // XXX: we loose 1 error message if both are failures
      (r1, r2) match {
        case (Success(v1), Success(v2)) => Success(v1 -> v2)
        case (Failure(e), _) => Failure(e)
        case (_, Failure(e)) => Failure(e)
      }
    }

    def or[Other >: To, B <: Other](alternative: Parser[B]) = Parser[Other] { r =>
      this(r).fold(
        e => alternative(r).fold(_ => Failure(e), s => Success(s)): ParseResult[Other],
        s => Success(s))
    }
  }

  sealed trait ParseResult[+To] {
    def fold[U](e: (String => U), s: (To => U)): U
  }
  case class Success[To](value: To) extends ParseResult[To] {
    override def fold[U](e: (String => U), s: (To => U)): U = s(value)
  }
  case class Failure(failure: String) extends ParseResult[Nothing] {
    override def fold[U](e: (String => U), s: (Nothing => U)): U = e(failure)
  }

  object Parser {

    def pure[To](v: To) = Parser[To](_ => Success(v))

    def apply[To](transformer: (Response => ParseResult[To])): Parser[To] = new Parser[To] {
      def apply(r: Response) = transformer(r)
    }

    def parse[To](r: Response)(implicit p: Parser[To]): ParseResult[To] = p(r)

    def resultParser[M <: Metadata, T](implicit mp: Parser[M], p: Parser[T]): Parser[Result[M, T]] = mp.flatMap { meta =>
      p.map { body =>
        Result(meta, body)
      }
    }

    implicit val unitParser: Parser[Unit] = Parser.pure(())

    implicit val emptyMetadataParser: Parser[EmptyMeta.type] = Parser.pure(EmptyMeta)

    def xmlErrorParser[M <: Metadata](implicit mp: Parser[M]) = mp.flatMap(meta => Parser[AWSError[M]] { r =>
      (r.status match {
        // TODO: really test content
        case s if (s / 100 == 2) => Some(Failure("Error expected, found success (status 2xx)"))
        case _ => for (
          code <- (r.xml \\ "Error" \ "Code").headOption.map(_.text);
          message <- (r.xml \\ "Error" \ "Message").headOption.map(_.text)
        ) yield Success(AWSError(meta, code, message))
      }).getOrElse(sys.error("Failed to parse error: " + r.body))
    })

  }
}
