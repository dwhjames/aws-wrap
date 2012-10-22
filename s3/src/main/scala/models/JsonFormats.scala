package aws.s3

import play.api.libs.json._
import play.api.libs.json.Json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.s3.models._
import Policy.Conditions.Condition

object JsonFormats {

  implicit def ConditionWrites[_] = Writes[Condition[_]](c =>
     Json.obj(
       c.name -> Json.toJson(
         c.values.map{ t =>
           t._1.name -> t._2.map(t._1.format.writes)}.toMap))
  )

  implicit val StatementWrites = Writes[Statement]{ s =>

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
  }

  implicit val PolicyWrites = Writes[Policy]( p =>
    Json.obj(
      "Version" -> p.version,
      "Id" -> p.id,
      "Statement" -> Json.toJson(p.statements))
  )

}