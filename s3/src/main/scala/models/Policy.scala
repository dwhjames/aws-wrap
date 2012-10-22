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
  conditions: Seq[Policy.Conditions.Condition[_, _]] = Nil)

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

    trait Condition[K <: Keys.Key[V], V] {
      import Conditions.Keys.Key

      def name: String
      def values: (Key[V], Seq[V])

      def or(other: Condition[K, V]): Condition[K, V] = {
        val c = this
        new Condition[K, V]{
          def name = c.name
          def values = c.values._1 -> (c.values._2 ++ other.values._2)
        }
      }

      override def toString = s"Condition($name, $values)"
    }

    object Keys {
      trait Key[T] {
        val name: String

        override def toString = s"Key($name)"
      }

      type IP = String
      type ARN = String

      object CURRENT_TIME extends Key[Date] { val name = "aws:CurrentTime" }
      object MULTI_FACTOR_AUTH_AGE extends Key[Long] { val name = "aws:MultiFactorAuthAge" }
      object SECURE_TRANSPORT extends Key[Boolean] { val name = "aws:SecureTransport" }
      object SOURCE_IP extends Key[IP]{ val name = "aws:SourceIp" }
      object USER_AGENT extends Key[String] { val name = "aws:UserAgent" }
      object EPOCH_TIME extends Key[Long] { val name = "aws:EpochTime" }
      object REFERER extends Key[String] { val name = "aws:Referer" }

      case class KeyFor[V](k: Key[V]) extends Key[Boolean] { val name = k.name }
    }

    import Conditions.Keys.Key

    case class Exists[V](values: (Keys.KeyFor[V], Seq[Boolean])) extends Condition[Keys.KeyFor[V], Boolean] {
      def name = "Null"
    }

    object Strings {
      case class Equals[K <: Key[String]](values: (K, Seq[String])) extends Condition[K, String] {
        def name = "StringEquals"
      }
      case class NotEquals[K <: Key[String]](values: (K, Seq[String])) extends Condition[K, String] {
        def name = "StringNotEquals"
      }
      /*
      case class EqualsIgnoreCase(values: (Key[String], Seq[String])*) extends Condition[EqualsIgnoreCase, String] {
        def name = "StringEqualsIgnoreCase"
      }
      case class NotEqualsIgnoreCase(values: (Key[String], Seq[String])*) extends Condition[NotEqualsIgnoreCase, String] {
        def name = "StringNotEqualsIgnoreCase"
      }
      case class Like(values: (Key[String], Seq[String])*) extends Condition[Like, String] {
        def name = "StringLike"
      }
      case class NotLike(values: (Key[String], Seq[String])*) extends Condition[NotLike, String] {
        def name = "StringNotLike"
      }
      */
    }

    /*
    object Nums {
      case class Equals(values: (Key[Number], Seq[Number])*) extends Condition[Equals, Number] {
        def name = "NumericEquals"
      }
      case class NotEquals(values: (Key[Number], Seq[Number])*) extends Condition[NotEquals, Number] {
        def name = "NumericNotEquals"
      }
      case class LessThan(values: (Key[Number], Seq[Number])*) extends Condition[LessThan, Number] {
        def name = "NumericLessThan"
      }
      case class LessThanEquals(values: (Key[Number], Seq[Number])*) extends Condition[LessThanEquals, Number] {
        def name = "NumericLessThanEquals"
      }
      case class GreaterThan(values: (Key[Number], Seq[Number])*) extends Condition[GreaterThan, Number] {
        def name = "NumericGreaterThan"
      }
      case class GreaterThanEquals(values: (Key[Number], Seq[Number])*) extends Condition[GreaterThanEquals, Number] {
        def name = "NumericGreaterThanEquals"
      }
    }

    object Dates {
      case class Equals(values: (Key[Date], Seq[Date])*) extends Condition[Equals, Date] {
        def name = "DateEquals"
      }
      case class NotEquals(values: (Key[Date], Seq[Date])*) extends Condition[NotEquals, Date] {
        def name = "DateNotEquals"
      }
      case class LessThan(values: (Key[Date], Seq[Date])*) extends Condition[LessThan, Date] {
        def name = "DateLessThan"
      }
      case class LessThanEquals(values: (Key[Date], Seq[Date])*) extends Condition[LessThanEquals, Date] {
        def name = "DateLessThanEquals"
      }
      case class GreaterThan(values: (Key[Date], Seq[Date])*) extends Condition[GreaterThan, Date] {
        def name = "DateGreaterThan"
      }
      case class GreaterThanEquals(values: (Key[Date], Seq[Date])*) extends Condition[GreaterThanEquals, Date] {
        def name = "DateGreaterThanEquals"
      }
    }

    object Booleans {
      case class Equals(values: (Key[Boolean], Seq[Boolean])*) extends Condition[Equals, Boolean] {
        def name = "Bool"
      }
    }

    object IPS {
      import Keys.IP
      case class Equals(values: (Key[IP], Seq[IP])*) extends Condition[Equals, IP] {
        def name = "IpAddress"
      }
      case class NotEquals(values: (Key[IP], Seq[IP])*) extends Condition[NotEquals, IP] {
        def name = "NotIpAddress"
      }
    }

    object ARNS {
      import Keys.ARN
      case class Equals(values: (Key[ARN], Seq[ARN])*) extends Condition[Equals, ARN] {
        def name = "ArnEquals"
      }
      case class NotEquals(values: (Key[ARN], Seq[ARN])*) extends Condition[NotEquals, ARN] {
        def name = "ArnNotEquals"
      }
      case class Like(values: (Key[ARN], Seq[ARN])*) extends Condition[Like, ARN] {
        def name = "ArnLike"
      }
      case class NotLike(values: (Key[ARN], Seq[ARN])*) extends Condition[NotLike, ARN] {
        def name = "ArnNotLike"
      }
    }
    */
  }

  def create(bucketname: String, policy: Policy) = {
    val body = Json.toJson(policy)
    println(body)
    request[Unit](PUT, Some(bucketname), body = Some(body.toString), subresource = Some("policy"))
  }
}