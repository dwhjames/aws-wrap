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

import play.api.libs.json._

case class PolicyStatement(
  effect:     PolicyEffect.Value,
  sid:        Option[String],
  principal:  Option[(String, Seq[String])] = None, // TODO: validate ?
  action:     Seq[String],
  notAction:  Seq[String]                   = Nil,
  resource:   Seq[String],
  conditions: Seq[PolicyCondition[_]]       = Nil
)

object PolicyStatement {

  // This is necessary because AWS returns single element arrays, as single values
  // {"foo": ["bar"]} is serialized as {"foo": "bar"}
  private implicit def awsSeqReads[T](implicit r: Reads[T]) = Reads[Seq[T]] {
    case JsArray(a) => JsSuccess(a.map(_.as[T]))
    case json       => r.reads(json).map(Seq(_))
  }

  implicit val policyStatementFormat: Format[PolicyStatement] = Format[PolicyStatement](
    Reads[PolicyStatement] { (json: JsValue) =>
      JsSuccess(
        PolicyStatement(
          effect     = PolicyEffect.withName((json \ "Effect").as[String]),
          sid        = (json \ "Sid")      .as[Option[String]],
          principal  = (json \ "Principal").as[Map[String, Seq[String]]].toSeq.headOption,
          action     = (json \ "Action")   .as[Option[Seq[String]]].getOrElse(Nil),
          notAction  = (json \ "NotAction").as[Option[Seq[String]]].getOrElse(Nil),
          resource   = (json \ "Resource") .as[Option[Seq[String]]].getOrElse(Nil),
          conditions = (json \ "Condition").as[Seq[PolicyCondition[_]]]
        )
      )
    },
    Writes[PolicyStatement] { s =>
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
