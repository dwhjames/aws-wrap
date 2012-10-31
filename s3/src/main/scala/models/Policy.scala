package aws.s3.models

import java.util.Date

import play.api.libs.ws._

import scala.concurrent.Future
import scala.xml.Elem

import play.api.libs.json._

import aws.core._
import aws.core.Types._
import aws.core.parsers.Parser

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

import aws.s3.S3.Parameters.Permisions.Grantees._

case class Statement(
  effect: Policy.Effects.Effect,
  sid: Option[String],
  principal: Option[(String, Seq[String])] = None, // TODO: validate ?
  action: Seq[String],
  notAction: Seq[String] = Nil,
  resource: Seq[String],
  conditions: Seq[Policy.Conditions.Condition[_]] = Nil)

case class Policy(
  id: Option[String],
  version: Option[String] = Some("2008-10-17"),
  statements: Seq[Statement])

object Policy {
  import Http._

  import aws.s3.JsonFormats._

  object Effects extends Enumeration {
    type Effect = Value
    val ALLOW = Value("Allow")
    val DENY = Value("Deny")
  }

  object Conditions {

    trait Condition[V] {
      parent =>

      import Conditions.Keys.Key

      def name: String
      def values: Seq[(Key[V], Seq[V])]

      def and(k: Key[V], u: V*): Condition[V] = new Condition[V] {
        def name = parent.name
        def values = parent.values :+ (k -> u)
      }

      override def toString = s"Condition($name, $values)"
    }

    object Keys {
      trait Key[T] {
        val name: String
        val format: Format[T]
        override def toString = s"Key($name)"
      }

      def withName(s: String) =
        ALL.find(_.name == s).getOrElse{ throw new RuntimeException(s"Unknown Key name: $s") }

      type IP = String
      type ARN = String

      val ALL = Seq[Key[_]](
        CURRENT_TIME,
        MULTI_FACTOR_AUTH_AGE,
        SECURE_TRANSPORT,
        SOURCE_IP,
        USER_AGENT,
        EPOCH_TIME,
        REFERER)

      object CURRENT_TIME extends Key[Date] {
        val name = "aws:CurrentTime"
        val format = implicitly[Format[Date]]
      }
      object MULTI_FACTOR_AUTH_AGE extends Key[Long] {
        val name = "aws:MultiFactorAuthAge"
        val format = implicitly[Format[Long]]
      }
      object SECURE_TRANSPORT extends Key[Boolean] {
        val name = "aws:SecureTransport"
        val format = implicitly[Format[Boolean]]
      }
      object SOURCE_IP extends Key[IP]{
        val name = "aws:SourceIp"
        val format = implicitly[Format[IP]]
      }
      object USER_AGENT extends Key[String] {
        val name = "aws:UserAgent"
        val format = implicitly[Format[String]]
      }
      object EPOCH_TIME extends Key[Long] {
        val name = "aws:EpochTime"
        val format = implicitly[Format[Long]]
      }
      object REFERER extends Key[String] {
        val name = "aws:Referer"
        val format = implicitly[Format[String]]
      }
      case class KeyFor[V](k: Key[V]) extends Key[Boolean] {
        val name = k.name
        val format = implicitly[Format[Boolean]]
      }
    }

    import Conditions.Keys.Key

    case class Exists[V](values: (Keys.KeyFor[V], Seq[Boolean])*) extends Condition[Boolean] {
      def name = "Null"
    }

    class ConditionBuilder[A](n: String) {
      def apply(v: (Key[A], Seq[A])*): Condition[A] = new Condition[A] {
        def name = n
        def values = v
      }
    }

    object Strings {
      val Equals = new ConditionBuilder[String]("StringEquals")
      val NotEquals = new ConditionBuilder[String]("StringNotEquals")
      val EqualsIgnoreCase = new ConditionBuilder[String]("StringEqualsIgnoreCase")
      val NotEqualsIgnoreCase = new ConditionBuilder[String]("StringNotEqualsIgnoreCase")
      val Like = new ConditionBuilder[String]("StringLike")
      val NotLike = new ConditionBuilder[String]("StringNotLike")
    }


    object Nums {
      val Equals = new ConditionBuilder[Number]("NumericEquals")
      val NotEquals = new ConditionBuilder[Number]("NumericNotEquals")
      val LessThan = new ConditionBuilder[Number]("NumericLessThan")
      val LessThanEquals = new ConditionBuilder[Number]("NumericLessThanEquals")
      val GreaterThan = new ConditionBuilder[Number]("NumericGreaterThan")
      val GreaterThanEquals = new ConditionBuilder[Number]("NumericGreaterThanEquals")
    }


    object Dates {
      val Equals = new ConditionBuilder[Date]("DateEquals")
      val NotEquals = new ConditionBuilder[Date]("DateNotEquals")
      val LessThan = new ConditionBuilder[Date]("DateLessThan")
      val LessThanEquals = new ConditionBuilder[Date]("DateLessThanEquals")
      val GreaterThan = new ConditionBuilder[Date]("DateGreaterThan")
      val GreaterThanEquals = new ConditionBuilder[Date]("DateGreaterThanEquals")
    }

    object Booleans {
      val Equals = new ConditionBuilder[Boolean]("Bool")
    }

    object IPS {
      import Keys.IP
      val Equals = new ConditionBuilder[IP]("IpAddress")
      val NotEquals = new ConditionBuilder[IP]("NotIpAddress")
    }

    object ARNS {
      import Keys.ARN
      val Equals = new ConditionBuilder[ARN]("ArnEquals")
      val NotEquals = new ConditionBuilder[ARN]("ArnNotEquals")
      val Like = new ConditionBuilder[ARN]("ArnLike")
      val NotLike = new ConditionBuilder[ARN]("ArnNotLike")
    }
  }

  def create(bucketname: String, policy: Policy) = {
    val b = Json.toJson(policy)
    put[JsValue, Unit](Some(bucketname), body = b, subresource = Some("policy"))
  }

  def get(bucketname: String) =
    Http.get[Policy](Some(bucketname), subresource = Some("policy"))

  def delete(bucketname: String) =
    Http.delete[Unit](Some(bucketname), subresource = Some("policy"))

}