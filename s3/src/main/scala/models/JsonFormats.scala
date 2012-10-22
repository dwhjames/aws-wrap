package aws.s3

import play.api.libs.json._
import play.api.libs.json.Json._
import play.api.libs.json.util._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

import aws.s3.models._
import Policy.Conditions.Condition

// case class Condition(name: String, values: Seq[(String, String)])

object JsonFormats {

  //implicit def ConditionWrites[C, V] = Writes[Condition[C, V]](c =>
  //   Json.obj(
  //     c.name -> Json.arr(
  //       c.values.map{ t => Json.obj(t._1 -> t._2)}))
  // )

  implicit val StatementWrites = Writes[Statement]( s =>
    Json.obj(
      "Effect" -> s.effect.toString,
      "Sid" -> s.sid,
      "Principal" -> Json.toJson(s.principal.map{ p => Json.obj(p._1 -> Json.toJson(p._2))}),
      "Action" -> Json.toJson(s.action),
      //"NotAction" -> Json.toJson(s.notAction),
      "Resource" -> Json.toJson(s.resource.map(_.toLowerCase))//,
      //"Condition" -> Json.toJson(s.conditions)
    )
  )

  implicit val PolicyWrites = Writes[Policy]( p =>
    Json.obj(
      "Version" -> p.version,
      "Id" -> p.id,
      "Statement" -> Json.toJson(p.statements))
  )

}