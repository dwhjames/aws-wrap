package controllers

import scala.concurrent.Future

import play.api._
import play.api.mvc._

import play.api.libs.Comet
import play.api.libs.Comet.CometMessage
import play.api.libs.iteratee._
import play.api.libs.concurrent._

import aws.core.{Result => AWSResult, AWSError}
import aws.sqs._
import aws.sqs.SQS.Queue

import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.commons.lang3.StringEscapeUtils

object Application extends Controller {

  val QUEUE = Queue("https://sqs.us-east-1.amazonaws.com/056023575103/streaming-sample")

  def index = Action {
    Ok(views.html.index())
  }

  def messageStream = Action {
    Ok.stream(messagesEnum &> Comet(callback = "parent.sqsMessageReceive"))
  }

  lazy val messagesEnum = QUEUE.messageEnumerator()

  implicit val sqsComet = CometMessage[MessageReceive](msg => "'" + StringEscapeUtils.escapeEcmaScript(msg.body) + "'")

}