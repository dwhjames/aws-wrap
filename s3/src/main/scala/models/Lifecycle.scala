package aws.s3.models

import java.util.Date

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.xml.Elem

import aws.core._
import aws.core.Types._

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

case class LifecycleConf(id: Option[String], prefix: String, status: LifecycleConf.Statuses.Status, lifetime: Duration)
object LifecycleConf {
  import Http._
  import Parameters._
  import Permisions._
  import ACLs._

  object Statuses extends Enumeration {
    type Status = Value
    val ENABLED = Value("Enabled")
    val DISABLED = Value("Disabled")
  }
  import Statuses._

  def create(bucketname: String, confs: LifecycleConf*): Future[EmptyResult[S3Metadata]] = {
    val body =
      <LifecycleConfiguration>
        {
          for (c <- confs) yield <Rule>
                                   {
                                     for (i <- c.id.toSeq)
                                       yield <ID>{ i }</ID>
                                   }
                                   <Prefix>{ c.prefix }</Prefix>
                                   <Status>{ c.status }</Status>
                                   <Expiration>
                                     <Days>{ c.lifetime.toDays }</Days>
                                   </Expiration>
                                 </Rule>
        }
      </LifecycleConfiguration>

    val ps = Seq(Parameters.MD5(body.mkString))
    request[Unit](PUT, Some(bucketname), body = Some(enumString(body.mkString)), subresource = Some("lifecycle"), parameters = ps)
  }

  def get(bucketname: String): Future[Result[S3Metadata, Seq[LifecycleConf]]] =
    request[Seq[LifecycleConf]](GET, Some(bucketname), subresource = Some("lifecycle"))

  def delete(bucketname: String): Future[EmptyResult[S3Metadata]] =
    request[Unit](DELETE, Some(bucketname), subresource = Some("lifecycle"))
}