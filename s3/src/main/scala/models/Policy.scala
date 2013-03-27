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

import aws.core.parsers.{Parser, Success}

case class Statement(
  effect:     PolicyEffect.Value,
  sid:        Option[String],
  principal:  Option[(String, Seq[String])]       = None, // TODO: validate ?
  action:     Seq[String],
  notAction:  Seq[String]                         = Nil,
  resource:   Seq[String],
  conditions: Seq[Policy.Conditions.Condition[_]] = Nil
)

object Statement {

  // This is necessary because AWS returns single element arrays, as single values
  // {"foo": ["bar"]} is serialized as {"foo": "bar"}
  private implicit def awsSeqReads[T](implicit r: Reads[T]) = Reads[Seq[T]] {
    case JsArray(a) => JsSuccess(a.map(_.as[T]))
    case json       => r.reads(json).map(Seq(_))
  }

  implicit val StatementFormat: Format[Statement] = Format[Statement](
    Reads[Statement] { (json: JsValue) =>
      JsSuccess(
        Statement(
          effect     = PolicyEffect.withName((json \ "Effect").as[String]),
          sid        = (json \ "Sid")      .as[Option[String]],
          principal  = (json \ "Principal").as[Map[String, Seq[String]]].toSeq.headOption,
          action     = (json \ "Action")   .as[Option[Seq[String]]].getOrElse(Nil),
          notAction  = (json \ "NotAction").as[Option[Seq[String]]].getOrElse(Nil),
          resource   = (json \ "Resource") .as[Option[Seq[String]]].getOrElse(Nil),
          conditions = (json \ "Condition").as[Seq[Policy.Conditions.Condition[_]]]
        )
      )
    },
    Writes[Statement] { s =>
      val ss = Seq[(String, JsValue)](
        "Effect"    -> Json.toJson(s.effect.toString),
        "Sid"       -> Json.toJson(s.sid),
        "Principal" -> Json.toJson(s.principal.map{ p => Json.obj(p._1 -> Json.toJson(p._2))}),
        "Resource"  -> Json.toJson(s.resource.map(_.toLowerCase)),
        "Condition" -> s.conditions.foldLeft(Json.obj()){ (obj, c) =>
            obj ++ Json.toJson(c).asInstanceOf[JsObject] // XXX
          }
      ) ++
      // GIVE ME A COMONAD
      (if(s.action.isEmpty)    Nil else Seq("Action"    -> Json.toJson(s.action))) ++
      (if(s.notAction.isEmpty) Nil else Seq("NotAction" -> Json.toJson(s.notAction)))

      Json.toJson(ss.toMap)
    }
  )
}

case class Policy(
  id:         Option[String],
  version:    Option[String]  = Some("2008-10-17"),
  statements: Seq[Statement]
)

object Policy {

  implicit def policyParser = Parser[Policy] { r =>
    Success(r.json.as[Policy])
  }

  implicit val PolicyFormat: Format[Policy] =
    Format[Policy](
      Reads[Policy] { (json: JsValue) =>
        JsSuccess(
          Policy(
            id         = (json \ "Id")       .as[Option[String]],
            version    = (json \ "Version")  .as[Option[String]],
            statements = (json \ "Statement").as[Seq[Statement]]
          )
        )
      },
      Writes[Policy] { p =>
        Json.obj(
          "Version"   -> p.version,
          "Id"        -> p.id,
          "Statement" -> Json.toJson(p.statements))
      }
    )

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

    object Condition {

      implicit val conditionsReads: Reads[Seq[Condition[_]]] =
        Reads[Seq[Condition[_]]] {
          case JsObject(o) =>
            JsSuccess {
              o map { case (name, vs) =>
                new ConditionBuilder[Any](name)(
                  vs.as[Map[String, Seq[JsValue]]]
                    .toSeq
                    .map{ t =>
                      val (k, vals) = t
                      val key = Keys.withName(k).asInstanceOf[Keys.Key[Any]]
                      // Handle Exist Condition
                      val rk = if(name == "Null") Keys.KeyFor(key) else key
                      key -> vals.map { v =>
                        rk.format.reads(v).get
                      }
                    }.head
                )
              }
            }
          case _ => JsError("Expected JsObject")
        }

      implicit val conditionWrites: Writes[Condition[_]] =
        Writes[Condition[_]] { c =>
          Json.obj(
            c.name -> Json.toJson(
              c.values.map{ t =>
                t._1.name -> t._2.map(t._1.format.writes)
              }.toMap
            )
          )
        }
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
}
