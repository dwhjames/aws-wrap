package aws.core

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
  def flatMap[T2](f: (T) => Result[M, T2]): Result[M, T2]
  def foreach(f: (T => Unit)): Unit
  override def toString = "Result(%s, %s, %s)".format(metadata, body)
}

object Result {
  def apply[M <: Metadata, T](m: M = EmptyMeta, b: T) = new Result[M, T] {
    override def toEither = Right(this)
    override def metadata = m
    override def body = b
    override def flatMap[T2](f: (T) => Result[M, T2]) = f(this.body)
    override def foreach(f: (T => Unit)) = f(b)
  }
  def unapply[M <: Metadata, T](r: Result[M, T]): Option[(M, T)] = Some(r.metadata -> r.body)
}

object EmptyResult {
  def apply[M <: Metadata](m: M = EmptyMeta) = Result.apply(m, ())
}

case class AWSError(code: String, message: String)
class Errors[M <: Metadata](val metadata: M, val errors: Seq[AWSError]) extends Result[M, Nothing] {
  override def toEither = Left(this)
  override def flatMap[T2](f: (Nothing) => Result[M, T2]) = this
  override def body = throw new RuntimeException(errors.toString)
  override def foreach(f: (Nothing => Unit)) = ()
}

object Errors {
  def apply[M <: Metadata](metadata: M = EmptyMeta, errors: Seq[AWSError]) = new Errors(metadata, errors)
  def unapply[M <: Metadata](e: Errors[M]): Option[Seq[AWSError]] = Some(e.errors)
}
// TODO: AWS sometimes returns a 200 when there is an error (example: NoSuchDomain error for DomainMetadata)
