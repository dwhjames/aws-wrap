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

package aws.s3
package models

import java.util.Date

import play.api.libs.json._

trait PolicyCondition[V] {
  parent =>

  import PolicyCondition.Key

  def name:   String
  def values: Seq[(Key[V], Seq[V])]

  def and(k: Key[V], u: V*): PolicyCondition[V] = new PolicyCondition[V] {
    def name   = parent.name
    def values = parent.values :+ (k -> u)
  }

  override def toString = s"PolicyCondition($name, $values)"
}

object PolicyCondition {

  implicit val policyConditionsReads: Reads[Seq[PolicyCondition[_]]] =
    Reads[Seq[PolicyCondition[_]]] {
      case JsObject(o) =>
        JsSuccess {
          o map { case (name, vs) =>
            new PolicyConditions.PolicyConditionBuilder[Any](name)(
              vs.as[Map[String, Seq[JsValue]]]
                .toSeq
                .map{ t =>
                  val (k, vals) = t
                  val key = Key.withName(k).asInstanceOf[Key[Any]]
                  // Handle Exist PolicyCondition
                  val rk = if(name == "Null") Key.KeyFor(key) else key
                  key -> vals.map { v =>
                    rk.format.reads(v).get
                  }
                } : _*
            )
          }
        }
      case _ => JsError("Expected JsObject")
    }

  implicit val policyConditionWrites: Writes[PolicyCondition[_]] =
    Writes[PolicyCondition[_]] { c =>
      Json.obj(
        c.name -> Json.toJson(
          c.values.map{ t =>
            t._1.name -> t._2.map(t._1.format.writes)
          }.toMap
        )
      )
    }

  trait Key[T] {
    val name:   String
    val format: Format[T]
    override def toString = s"Key($name)"
  }

  object Key {

    def withName(s: String) =
      ALL find (_.name == s) getOrElse { throw new RuntimeException(s"Unknown Key name: $s") }

    type IP = String
    type ARN = String

    val ALL = Seq[Key[_]](
      CURRENT_TIME,
      MULTI_FACTOR_AUTH_AGE,
      SECURE_TRANSPORT,
      SOURCE_IP,
      USER_AGENT,
      EPOCH_TIME,
      REFERER
    )

    object CURRENT_TIME extends Key[Date] {
      val name   = "aws:CurrentTime"
      val format = implicitly[Format[Date]]
    }
    object MULTI_FACTOR_AUTH_AGE extends Key[Long] {
      val name   = "aws:MultiFactorAuthAge"
      val format = implicitly[Format[Long]]
    }
    object SECURE_TRANSPORT extends Key[Boolean] {
      val name   = "aws:SecureTransport"
      val format = implicitly[Format[Boolean]]
    }
    object SOURCE_IP extends Key[IP]{
      val name   = "aws:SourceIp"
      val format = implicitly[Format[IP]]
    }
    object USER_AGENT extends Key[String] {
      val name   = "aws:UserAgent"
      val format = implicitly[Format[String]]
    }
    object EPOCH_TIME extends Key[Long] {
      val name   = "aws:EpochTime"
      val format = implicitly[Format[Long]]
    }
    object REFERER extends Key[String] {
      val name   = "aws:Referer"
      val format = implicitly[Format[String]]
    }
    case class KeyFor[V](k: Key[V]) extends Key[Boolean] {
      val name   = k.name
      val format = implicitly[Format[Boolean]]
    }
  }

}

object PolicyConditions {

  import PolicyCondition.Key

  case class Exists[V](values: (Key.KeyFor[V], Seq[Boolean])*) extends PolicyCondition[Boolean] {
    def name = "Null"
  }

  class PolicyConditionBuilder[A](n: String) {
    def apply(v: (Key[A], Seq[A])*): PolicyCondition[A] = new PolicyCondition[A] {
      def name = n
      def values = v
    }
  }

  object Strings {
    val Equals              = new PolicyConditionBuilder[String]("StringEquals")
    val NotEquals           = new PolicyConditionBuilder[String]("StringNotEquals")
    val EqualsIgnoreCase    = new PolicyConditionBuilder[String]("StringEqualsIgnoreCase")
    val NotEqualsIgnoreCase = new PolicyConditionBuilder[String]("StringNotEqualsIgnoreCase")
    val Like                = new PolicyConditionBuilder[String]("StringLike")
    val NotLike             = new PolicyConditionBuilder[String]("StringNotLike")
  }


  object Nums {
    val Equals            = new PolicyConditionBuilder[Number]("NumericEquals")
    val NotEquals         = new PolicyConditionBuilder[Number]("NumericNotEquals")
    val LessThan          = new PolicyConditionBuilder[Number]("NumericLessThan")
    val LessThanEquals    = new PolicyConditionBuilder[Number]("NumericLessThanEquals")
    val GreaterThan       = new PolicyConditionBuilder[Number]("NumericGreaterThan")
    val GreaterThanEquals = new PolicyConditionBuilder[Number]("NumericGreaterThanEquals")
  }


  object Dates {
    val Equals            = new PolicyConditionBuilder[Date]("DateEquals")
    val NotEquals         = new PolicyConditionBuilder[Date]("DateNotEquals")
    val LessThan          = new PolicyConditionBuilder[Date]("DateLessThan")
    val LessThanEquals    = new PolicyConditionBuilder[Date]("DateLessThanEquals")
    val GreaterThan       = new PolicyConditionBuilder[Date]("DateGreaterThan")
    val GreaterThanEquals = new PolicyConditionBuilder[Date]("DateGreaterThanEquals")
  }

  object Booleans {
    val Equals = new PolicyConditionBuilder[Boolean]("Bool")
  }

  object IPS {
    import Key.IP
    val Equals    = new PolicyConditionBuilder[IP]("IpAddress")
    val NotEquals = new PolicyConditionBuilder[IP]("NotIpAddress")
  }

  object ARNS {
    import Key.ARN
    val Equals    = new PolicyConditionBuilder[ARN]("ArnEquals")
    val NotEquals = new PolicyConditionBuilder[ARN]("ArnNotEquals")
    val Like      = new PolicyConditionBuilder[ARN]("ArnLike")
    val NotLike   = new PolicyConditionBuilder[ARN]("ArnNotLike")
  }
}
