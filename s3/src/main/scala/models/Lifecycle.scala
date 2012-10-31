package aws.s3.models

import java.util.Date

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.xml._

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
    val b =
      <LifecycleConfiguration>
        {
          for (c <- confs) yield
            <Rule>
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

    val ps = Seq(Parameters.MD5(b.mkString))
    put[Node, Unit](Some(bucketname), body = b, subresource = Some("lifecycle"), parameters = ps)
  }

  def get(bucketname: String): Future[Result[S3Metadata, Seq[LifecycleConf]]] =
    Http.get[Seq[LifecycleConf]](Some(bucketname), subresource = Some("lifecycle"))

  def delete(bucketname: String): Future[EmptyResult[S3Metadata]] =
    Http.delete[Unit](Some(bucketname), subresource = Some("lifecycle"))
}