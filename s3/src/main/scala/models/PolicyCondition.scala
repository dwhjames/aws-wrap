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

trait PolicyCondition {
  parent =>

  import PolicyCondition.Key

  def name:   String
  def values: Seq[(Key, Seq[Any])]

  def and[V](k: Key, u: V*): PolicyCondition = new PolicyCondition {
    def name   = parent.name
    def values = parent.values :+ (k -> u)
  }

  override def toString = s"PolicyCondition($name, $values)"
}

object PolicyCondition {

  implicit val policyConditionsReads: Reads[Seq[PolicyCondition]] =
    Reads[Seq[PolicyCondition]] {
      case JsObject(o) =>
        JsSuccess {
          o map { case (name, vs) =>
            new PolicyConditions.PolicyConditionBuilder(name)(
              vs.as[Map[String, Seq[JsValue]]]
                .toSeq
                .map { case (k, vals) =>
                  val key = Key.withName(k)
                  // Handle Exist PolicyCondition
                  val rk = if(name == "Null") Key.KeyFor(key) else key
                  rk -> vals.map { v =>
                    rk.format.reads(v).get
                  }
                } : _*
            )
          }
        }
      case _ => JsError("Expected JsObject")
    }

  implicit val policyConditionWrites: Writes[PolicyCondition] =
    Writes[PolicyCondition] { c =>
      Json.obj(
        c.name -> Json.toJson(
          c.values.map{ case (k, vals) =>
            k.name -> vals.map(x => k.writeVal(x))
          }.toMap
        )
      )
    }

  trait Key {
    type T

    val name:   String
    val format: Format[T]

    def writeVal(x: Any): JsValue = format.writes(x.asInstanceOf[T])
    override def toString = s"Key($name)"
  }

  object Key {

    def withName(s: String): Key =
      ALL find (_.name == s) getOrElse { throw new RuntimeException(s"Unknown Key name: $s") }

    type IP = String
    type ARN = String

    val ALL = Seq[Key](
      CURRENT_TIME,
      MULTI_FACTOR_AUTH_AGE,
      SECURE_TRANSPORT,
      SOURCE_IP,
      USER_AGENT,
      EPOCH_TIME,
      REFERER
    )

    object CURRENT_TIME extends Key {
      type T = Date
      val name   = "aws:CurrentTime"
      val format = implicitly[Format[Date]]
    }
    object MULTI_FACTOR_AUTH_AGE extends Key {
      type T = Long
      val name   = "aws:MultiFactorAuthAge"
      val format = implicitly[Format[Long]]
    }
    object SECURE_TRANSPORT extends Key {
      type T = Boolean
      val name   = "aws:SecureTransport"
      val format = implicitly[Format[Boolean]]
    }
    object SOURCE_IP extends Key {
      type T = IP
      val name   = "aws:SourceIp"
      val format = implicitly[Format[IP]]
    }
    object USER_AGENT extends Key {
      type T = String
      val name   = "aws:UserAgent"
      val format = implicitly[Format[String]]
    }
    object EPOCH_TIME extends Key {
      type T = Long
      val name   = "aws:EpochTime"
      val format = implicitly[Format[Long]]
    }
    object REFERER extends Key {
      type T = String
      val name   = "aws:Referer"
      val format = implicitly[Format[String]]
    }
    case class KeyFor(k: Key) extends Key {
      type T = Boolean
      val name   = k.name
      val format = implicitly[Format[Boolean]]
    }
  }

}

object PolicyConditions {

  import PolicyCondition.Key

  case class Exists(values: (Key.KeyFor, Seq[Boolean])*) extends PolicyCondition {
    def name = "Null"
  }

  class PolicyConditionBuilder(n: String) {
    def apply(v: (Key, Seq[_])*): PolicyCondition = new PolicyCondition {
      def name = n
      def values = v
    }
  }

  object Strings {
    val Equals              = new PolicyConditionBuilder("StringEquals")
    val NotEquals           = new PolicyConditionBuilder("StringNotEquals")
    val EqualsIgnoreCase    = new PolicyConditionBuilder("StringEqualsIgnoreCase")
    val NotEqualsIgnoreCase = new PolicyConditionBuilder("StringNotEqualsIgnoreCase")
    val Like                = new PolicyConditionBuilder("StringLike")
    val NotLike             = new PolicyConditionBuilder("StringNotLike")
  }


  object Nums {
    val Equals            = new PolicyConditionBuilder("NumericEquals")
    val NotEquals         = new PolicyConditionBuilder("NumericNotEquals")
    val LessThan          = new PolicyConditionBuilder("NumericLessThan")
    val LessThanEquals    = new PolicyConditionBuilder("NumericLessThanEquals")
    val GreaterThan       = new PolicyConditionBuilder("NumericGreaterThan")
    val GreaterThanEquals = new PolicyConditionBuilder("NumericGreaterThanEquals")
  }


  object Dates {
    val Equals            = new PolicyConditionBuilder("DateEquals")
    val NotEquals         = new PolicyConditionBuilder("DateNotEquals")
    val LessThan          = new PolicyConditionBuilder("DateLessThan")
    val LessThanEquals    = new PolicyConditionBuilder("DateLessThanEquals")
    val GreaterThan       = new PolicyConditionBuilder("DateGreaterThan")
    val GreaterThanEquals = new PolicyConditionBuilder("DateGreaterThanEquals")
  }

  object Booleans {
    val Equals = new PolicyConditionBuilder("Bool")
  }

  object IPS {
    import Key.IP
    val Equals    = new PolicyConditionBuilder("IpAddress")
    val NotEquals = new PolicyConditionBuilder("NotIpAddress")
  }

  object ARNS {
    import Key.ARN
    val Equals    = new PolicyConditionBuilder("ArnEquals")
    val NotEquals = new PolicyConditionBuilder("ArnNotEquals")
    val Like      = new PolicyConditionBuilder("ArnLike")
    val NotLike   = new PolicyConditionBuilder("ArnNotLike")
  }
}
