package aws.s3.models

import java.util.Date

import scala.concurrent.Future
import scala.xml._

import aws.core._
import aws.core.Types._

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

object Events extends Enumeration {
  type Event = Value
  val REDUCED_REDUNDANCY_LOST_OBJECT = Value("s3:ReducedRedundancyLostObject")
}

case class NotificationConfiguration(topic: String, event: Events.Event)
object NotificationConfiguration {
  import Http._
  import Parameters._
  import Permisions._
  import ACLs._

  def create(bucketname: String, notification: NotificationConfiguration): Future[EmptyResult[S3Metadata]] = {
    val b =
      <NotificationConfiguration>
         <TopicConfiguration>
             <Topic>{ notification.topic }</Topic>
             <Event>{ notification.event.toString }</Event>
         </TopicConfiguration>
      </NotificationConfiguration>

    put[Node, Unit](Some(bucketname), body = b, subresource = Some("notification"))
  }

  def get(bucketname: String) =
    Http.get[Seq[NotificationConfiguration]](Some(bucketname), subresource = Some("notification"))
}