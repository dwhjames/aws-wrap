package aws.dynamodb

import play.api.libs.json._

sealed trait DDBResult

case object EmptyDDBResult extends DDBResult

trait SimpleDDBResult[T] extends DDBResult {
  def body: T
  override def toString = "SimpleResult(%s, %s)".format(body)
}

object SimpleDDBResult {
  def apply[T](json: JsValue)(implicit r: Reads[T]): SimpleDDBResult[T] = new SimpleDDBResult[T] {
    def body = r.reads(json) match {
      case JsSuccess(js, path) => js
      case JsError(e) => sys.error("Parsing error: " + e)
    }
  }

  def unapply[T](s: SimpleDDBResult[T]): Option[T] = Some(s.body)
}

