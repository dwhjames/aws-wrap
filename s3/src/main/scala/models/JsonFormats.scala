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

import play.api.libs.json._
import play.api.libs.json.Json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.s3.models._
import Policy.Conditions.Condition

object JsonFormats {

  // This is necessary because AWS returns single element arrays, as single values
  // {"foo": ["bar"]} is serialized as {"foo": "bar"}
  implicit def awsSeqReads[T](implicit r: Reads[T]) = Reads[Seq[T]]{ (json: JsValue) => json match {
    case JsArray(a) => JsSuccess(a.map(_.as[T]))
    case _ => r.reads(json).map(Seq(_))
  }}

  implicit val ConditionsReads: Reads[Seq[Condition[_]]] =
    Reads[Seq[Condition[_]]]{(json: JsValue) =>
      import Policy.Conditions._
      json match {
        case JsObject(o) =>
          JsSuccess(o.map{ case (name, vs) =>
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
          })
        case _ => JsError("Expected JsObject")
    }}

  implicit val ConditionWrites =
    Writes[Condition[_]](c =>
       Json.obj(
         c.name -> Json.toJson(
           c.values.map{ t =>
             t._1.name -> t._2.map(t._1.format.writes)}.toMap)))

  implicit val StatementFormat = Format[Statement](
    Reads[Statement]{(json: JsValue) =>
      JsSuccess(Statement(
        effect = PolicyEffect.withName((json \ "Effect").as[String]),
        sid = (json \ "Sid").as[Option[String]],
        principal = (json \ "Principal").as[Map[String, Seq[String]]].toSeq.headOption,
        action = (json \ "Action").as[Option[Seq[String]]].getOrElse(Nil),
        notAction = (json \ "NotAction").as[Option[Seq[String]]].getOrElse(Nil),
        resource = (json \ "Resource").as[Option[Seq[String]]].getOrElse(Nil),
        conditions = (json \ "Condition").as[Seq[Condition[_]]]
      ))
    },
    Writes[Statement]{ s =>
        val ss = Seq[(String, JsValue)](
            "Effect" -> Json.toJson(s.effect.toString),
            "Sid" -> Json.toJson(s.sid),
            "Principal" -> Json.toJson(s.principal.map{ p => Json.obj(p._1 -> Json.toJson(p._2))}),
            "Resource" -> Json.toJson(s.resource.map(_.toLowerCase)),
            "Condition" -> s.conditions.foldLeft(Json.obj()){ (obj, c) =>
                obj ++ Json.toJson(c).asInstanceOf[JsObject] // XXX
              }
          ) ++
          // GIVE ME A COMONAD
          (if(s.action.isEmpty) Nil else Seq("Action" -> Json.toJson(s.action))) ++
          (if(s.notAction.isEmpty) Nil else Seq("NotAction" -> Json.toJson(s.notAction)))

        Json.toJson(ss.toMap)
      })

  implicit val PolicyFormat = Format[Policy](
    Reads[Policy]((json: JsValue) =>
      JsSuccess(Policy(
        id = (json \ "Id").as[Option[String]],
        version = (json \ "Version").as[Option[String]],
        statements = (json \ "Statement").as[Seq[Statement]]
      ))
    ),
    Writes[Policy]( p =>
      Json.obj(
        "Version" -> p.version,
        "Id" -> p.id,
        "Statement" -> Json.toJson(p.statements))))

}
