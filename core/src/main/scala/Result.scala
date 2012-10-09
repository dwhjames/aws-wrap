package aws.core

import scala.util.{ Try, Success, Failure }
import scala.xml.Elem
import play.api.libs.ws.{ Response => WSResponse }

import aws.core.parsers._
import aws.core.parsers.Parser._

object Types {
  type EmptyResult[M <: Metadata] = Result[M, Unit]
}

trait Metadata
case object EmptyMeta extends Metadata

sealed trait Result[M <: Metadata, +T] {
  def metadata: M
  def body: T
  def toEither: Either[Errors[M], Result[M, T]]
  override def toString = "Result(%s, %s, %s)".format(metadata, body)
}

object Result {
  def apply[M <: Metadata, T](m: M = EmptyMeta, b: T) = new Result[M, T] {
    def toEither = Right(this)
    def metadata = m
    def body = b
  }
}

case class AWSError(code: String, message: String)
case class Errors[M <: Metadata](val metadata: M = EmptyMeta, errors: Seq[AWSError]) extends Result[M, Nothing] {
  def toEither = Left(this)
  def body = throw new RuntimeException(errors.toString)
}
// TODO: AWS sometimes returns a 200 when there is an error (example: NoSuchDomain error for DomainMetadata)
