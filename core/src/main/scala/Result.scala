package aws.core

import play.api.libs.ws.{ Response => WSResponse }

import aws.core.parsers._
import aws.core.parsers.Parser._

object Types {
  /**
   * A [[Result]] with no body, for calls not returning any body (example: deleting a resource)
   */
  type EmptyResult[M <: Metadata] = Result[M, Unit]
  /**
   * A [[Result]] with no metadata, for services that don't return query metadata.
   */
  type SimpleResult[T] = Result[EmptyMeta.type, T]
  /**
   * A [[Result]] with neither metadata nor body
   */
  type EmptySimpleResult = Result[EmptyMeta.type, Unit]
}

/**
 * Metadata returned in a response wether the result was an error or not. Usually related to the request.
 */
trait Metadata
case object EmptyMeta extends Metadata

/**
 * An AWS Result, that can be a success on a error. M represents the metadata (some information always there
 * whether the result is a success or an error) and T represents the type of the body in case of a success.
 *
 * Some services don't have any metadata with responses, in this case `EmptyMeta` will be returned.
 *
 * To consume it, you can use pattern matching:
 * {{{
 *   r match {
 *     case Result(metadata, body) => // Do something with the body
 *     case Errors(metadata, errors) => // Do something with the errors
 *   }
 * }}}
 */
sealed trait Result[M <: Metadata, +T] {
  /**
   * The metadata, `EmptyMeta` if the service doesn't support metadata.
   */
  def metadata: M
  /**
   * Return the body if a success, throws an exception if an error
   */
  def body: T
  def toEither: Either[Errors[M], Result[M, T]]
  def map[T2](f: (T) => T2): Result[M, T2]
  def flatMap[T2](f: (T) => Result[M, T2]): Result[M, T2]
  def foreach(f: (T => Unit)): Unit
  override def toString = "Result(%s, %s)".format(metadata, body)
}

object Result {
  def apply[M <: Metadata, T](m: M = EmptyMeta, b: T): Result[M, T] = new Result[M, T] {
    override def toEither = Right(this)
    override def metadata = m
    override def body = b
    override def map[T2](f: (T) => T2) = Result(this.metadata, f(this.body))
    override def flatMap[T2](f: (T) => Result[M, T2]) = f(this.body)
    override def foreach(f: (T => Unit)) = f(b)
  }
  def unapply[M <: Metadata, T](r: Result[M, T]): Option[(M, T)] = r match {
    case Errors(_) => None
    case _ => Some(r.metadata -> r.body)
  }

}

object EmptyResult {
  def apply[M <: Metadata](m: M = EmptyMeta) = Result.apply(m, ())
}

case class AWSError(code: String, message: String)

class Errors[M <: Metadata](val metadata: M, val errors: Seq[AWSError]) extends Result[M, Nothing] {
  override def toEither = Left(this)
  override def map[T2](f: (Nothing) => T2) = this
  override def flatMap[T2](f: (Nothing) => Result[M, T2]) = this
  override def body = throw new RuntimeException(errors.toString)
  override def foreach(f: (Nothing => Unit)) = ()
}

object Errors {
  def apply[M <: Metadata](metadata: M = EmptyMeta, errors: Seq[AWSError]) = new Errors(metadata, errors)
  def unapply[M <: Metadata](e: Errors[M]): Option[Seq[AWSError]] = Some(e.errors)
}
// TODO: AWS sometimes returns a 200 when there is an error (example: NoSuchDomain error for DomainMetadata)
