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

package aws.ses

import java.util.Date

import scala.concurrent.ExecutionContext.Implicits.global

import play.api.libs.ws._
import play.api.libs.ws.WS._

import aws.core._
import aws.core.Types._
import aws.core.parsers._
import aws.core.utils._

case class SESMetadata(requestId: String) extends Metadata
case class EmailResult(messageId: String)

object ContentTypes extends Enumeration {
  type ContentType = Value
  val HTML, PLAIN_TEXT, BOTH = Value
}

object Simulators {
  val SUCCESS       = "success@simulator.amazonses.com"   // The recipient’s ISP accepts your email and delivers it to the recipient’s inbox.
  val BOUNCE        = "bounce@simulator.amazonses.com"    // The recipient’s ISP rejects your email with an SMTP 550 5.1.1 response code ("Unknown User")
  val OUT_OF_OFFICE = "ooto@simulator.amazonses.com"      // The recipient’s ISP accepts your email and delivers it to the recipient’s inbox. The ISP sends an out-of-the-office (OOTO) message to Amazon SES.
  val COMPLAINT     = "complaint@simulator.amazonses.com" // The recipient’s ISP accepts your email and delivers it to the recipient’s inbox. The recipient, however, does not want to receive your message and clicks "Mark as Spam" within an email application that uses an ISP that sends a complaint response to Amazon SES.
  val BLACKLIST     = "blacklist@simulator.amazonses.com" // Your attempt to send the email fails with a MessageRejected exception that contains an “Address Blacklisted” error message.
}

sealed trait Destination {
  val name: String
  val address: String
}
case class To(address: String) extends Destination {
  override val name = "To"
}
case class CC(address: String) extends Destination {
  override val name = "Cc"
}
case class BCC(address: String) extends Destination {
  override val name = "Bcc"
}

object NotificationTypes extends Enumeration {
  type NotificationType = Value
  val BOUNCE = Value("Bounce")
  val COMPLAINT = Value("Complaint")
}

object Statuses extends Enumeration {
  type Status = Value
  val ENABLED = Value("true")
  val DISABLED = Value("false")
}

case class SendDataPoint(bounces: Long, complaints: Long, deliveryAttempts: Long, rejects: Long, timeStamp: Date)
case class SendQuota(max24HourSend: Double, maxSendRate: Double, sentLast24Hours: Double)
case class Email(subject: String, body: String, contentType: ContentTypes.ContentType, source: String, destinations: Seq[Destination], replyTo: Seq[String] = Nil, returnPath: Option[String] = None)

// TODO:
// DeleteIdentity
// DeleteVerifiedEmailAddress
// GetIdentityDkimAttributes
// GetIdentityNotificationAttributes
// GetIdentityVerificationAttributes

// DEPRECATED:
// VerifyEmailAddress
// ListVerifiedEmailAddresses


sealed trait Identity { val value: String }
object Identity {
  def apply(value: String) = value match {
    case s if s.contains("@") => EmailAddress(value)
    case _ => Domain(value)
  }
  def unapply(i: Identity) = Some(i.value)
}
case class EmailAddress(value: String) extends Identity
case class Domain(value: String) extends Identity

case class Paginated[T](entities: Seq[T], maxItems: Int, nextToken: Option[String])

object SES {

  type VerificationToken = String
  type DkimToken = String

  object Parameters {
    def Date(d: Date) = ("Date" -> AWS.httpDateFormat(d))
    def X_AMZN_AUTHORIZATION(accessKeyId: String, algorithm: String, signature: String) =
      ("X-Amzn-Authorization" -> s"AWS3-HTTPS AWSAccessKeyId=$accessKeyId,Algorithm=$algorithm,Signature=$signature")
  }

  private def tryParse[T](resp: Response)(implicit p: Parser[Result[SESMetadata, T]]) =
    Parser.parse[Result[SESMetadata, T]](resp).fold(e => throw new RuntimeException(e), identity)

  private def request[T](action: String, params: Seq[(String, String)] = Nil)(implicit region: SESRegion, p: Parser[Result[SESMetadata, T]]) = {
    val date = Parameters.Date(new Date)

    val signature = Crypto.base64(Crypto.hmacSHA1(date._2.getBytes(), AWS.secret))
    val allHeaders = Seq(
      date,
      Parameters.X_AMZN_AUTHORIZATION(AWS.key, "HmacSHA1", signature))

    val ps = (params :+ AWS.Parameters.Action(action) :+ AWS.Parameters.TimeStamp(new Date))
      .toMap.mapValues(Seq(_))

    WS.url(s"https://email.${region.subdomain}.amazonaws.com")
      .withHeaders(allHeaders: _*)
      .post(ps)
      .map(tryParse[T])
  }


  import aws.ses.SESParsers._

  def send(email: Email)(implicit region: SESRegion) = {
    val ps = Seq(
      "Source" -> email.source,
      "Message.Subject.Data" -> email.subject,
      "Message.Body.Text.Data" -> email.body
    ) ++
    email.destinations.groupBy(_.name).flatMap{ case (_, ds) =>
      ds.zipWithIndex.map { case (d, i) =>
        s"Destination.${d.name}Addresses.member.${i+1}" -> d.address
      }
    } ++
    email.replyTo.zipWithIndex.map{ case (r, i) =>
      s"ReplyToAddresses.member.${i+1}" -> r
    } ++
    email.returnPath.toSeq.map("ReturnPath" -> _)

    request[EmailResult]("SendEmail", ps)
  }

  def sendRaw(rawMessage: String)(implicit region: SESRegion) =
    request[EmailResult]("SendRawEmail", Seq(
      "RawMessage.Data" -> Crypto.base64(rawMessage.getBytes)
    ))

  def verifyEmailIdentity(email: String)(implicit region: SESRegion) =
    request[EmailResult]("VerifyEmailIdentity", Seq(
      "EmailAddress" -> email
    ))

  def verifyDomainIdentity(domain: String)(implicit region: SESRegion) =
    request[VerificationToken]("VerifyDomainIdentity", Seq(
      "Domain" -> domain
    ))

  def setDKIMSigningStatus(identity: String, status: Statuses.Status) =
    request[Unit]("SetIdentityDkimEnabled", Seq(
      "DkimEnabled" -> status.toString,
      "Identity" -> identity
    ))

  def verifyDomainDkim(domain: String)(implicit region: SESRegion) =
    request[Seq[DkimToken]]("VerifyDomainDkim", Seq(
      "Domain" -> domain
    ))

  def seIdentityFeedbackForwardingStatus(identity: String, status: Statuses.Status) =
    request[Unit]("SetIdentityFeedbackForwardingEnabled", Seq(
      "ForwardingEnabled" -> status.toString,
      "Identity" -> identity
    ))

  def setIdentityNotificationTopic(identity: String, topic: String, notifType: NotificationTypes.NotificationType) =
    request[Unit]("SetIdentityNotificationTopic", Seq(
      "Identity" -> identity,
      "SnsTopic" -> topic,
      "NotificationType" -> notifType.toString
    ))

  def listIdentities(nextToken: Option[String] = None) =
    request[Paginated[Identity]]("ListIdentities",
      nextToken.toSeq.map("NextToken" -> _))

  def sendStatistics() =
    request[Seq[SendDataPoint]]("GetSendStatistics")

  def sendQuota() =
    request[SendQuota]("GetSendQuota")
}
