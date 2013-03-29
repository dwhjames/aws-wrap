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

object VerificationStatuses extends Enumeration {
  type VerificationStatus = Value
  val PENDING = Value("Pending")
  val SUCCESS = Value("Success")
  val FAILED = Value("Failed")
  val TEMPORARY_FAILURE = Value("TemporaryFailure")
  val NOT_STARTED = Value("NotStarted")
}

case class SendDataPoint(bounces: Long, complaints: Long, deliveryAttempts: Long, rejects: Long, timeStamp: Date)
case class SendQuota(max24HourSend: Double, maxSendRate: Double, sentLast24Hours: Double)
case class Body(text: Option[String] = None, html: Option[String] = None)
case class Email(subject: String, body: Body, source: String, destinations: Seq[Destination], replyTo: Seq[String] = Nil, returnPath: Option[String] = None)
case class VerificationAttributes(status: VerificationStatuses.VerificationStatus, token: Option[SES.VerificationToken])
case class IdentityNotificationAttributes(forwardingEnabled: Boolean, bounceTopic: Option[String], complaintTopic: Option[String])
case class IdentityDkimAttributes(enabled: Boolean, status: VerificationStatuses.VerificationStatus, tokens: Seq[SES.DkimToken])

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

  /**
  * Composes an email message based on input data, and then immediately queues the message for sending.
  * @param email The email to be sent.
  */
  def send(email: Email)(implicit region: SESRegion) = {
    val ps = Seq(
      "Source" -> email.source,
      "Message.Subject.Data" -> email.subject
    ) ++
    email.body.text.toSeq.map("Message.Body.Text.Data" -> _) ++
    email.body.html.toSeq.map("Message.Body.Html.Data" -> _) ++
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

  /**
  * Sends an email message, with header and content specified by the client. The SendRawEmail action is useful for sending multipart MIME emails. The raw text of the message must comply with Internet email standards; otherwise, the message cannot be sent.
  * @param rawMessage The raw text of the message.
  */
  def sendRaw(rawMessage: String, source: Option[String] = None, destinations: Seq[Destination] = Nil)(implicit region: SESRegion) =
    request[EmailResult]("SendRawEmail", Seq(
      "RawMessage.Data" -> Crypto.base64(rawMessage.getBytes)
      ) ++
      source.toSeq.map("Source" -> _) ++
      destinations.zipWithIndex.map { case (dest, i) =>
        s"Destinations.member.${i+1}" -> dest.address
      }
    )

  /**
   * Verifies an email address. This action causes a confirmation email message to be sent to the specified address.
   * @param email The email address to be verified
   */
  def verifyEmailIdentity(email: String)(implicit region: SESRegion) =
    request[EmailResult]("VerifyEmailIdentity", Seq(
      "EmailAddress" -> email
    ))

  /**
   * Verifies a domain
   * @param domain The domain to be verified.
   */
  def verifyDomainIdentity(domain: String)(implicit region: SESRegion) =
    request[VerificationToken]("VerifyDomainIdentity", Seq(
      "Domain" -> domain
    ))

  /**
   * Enables or disables Easy DKIM signing of email sent from an identity
   * @param identity The identity for which DKIM signing should be enabled or disabled.
   * @param status The DKIM signing status
   */
  def setDKIMSigningStatus(identity: String, status: Statuses.Status)(implicit region: SESRegion) =
    request[Unit]("SetIdentityDkimEnabled", Seq(
      "DkimEnabled" -> status.toString,
      "Identity" -> identity
    ))

  /**
  * Returns a set of DKIM tokens for a domain.
  * @param domain The name of the domain to be verified for Easy DKIM signing.
  */
  def verifyDomainDkim(domain: String)(implicit region: SESRegion) =
    request[Seq[DkimToken]]("VerifyDomainDkim", Seq(
      "Domain" -> domain
    ))

  /**
  * Given an identity, enables or disables whether Amazon SES forwards feedback notifications as email.
  * @param identity The identity for which to set feedback notification forwarding.
  * @param status Sets whether Amazon SES will forward feedback notifications as email.
  */
  def setIdentityFeedbackForwardingStatus(identity: String, status: Statuses.Status)(implicit region: SESRegion) =
    request[Unit]("SetIdentityFeedbackForwardingEnabled", Seq(
      "ForwardingEnabled" -> status.toString,
      "Identity" -> identity
    ))

  /**
  * Given an identity (email address or domain), sets the Amazon SNS topic to which Amazon SES will publish bounce and complaint notifications for emails sent with that identity as the Source.
  * @param identity The identity for which the topic will be set.
  * @param topic The Amazon Resource Name (ARN) of the Amazon Simple Notification Service (Amazon SNS) topic.
  * @param notifType The type of feedback notifications that will be published to the specified topic.
  */
  def setIdentityNotificationTopic(identity: String, topic: String, notifType: NotificationTypes.NotificationType)(implicit region: SESRegion) =
    request[Unit]("SetIdentityNotificationTopic", Seq(
      "Identity" -> identity,
      "SnsTopic" -> topic,
      "NotificationType" -> notifType.toString
    ))

  /**
  * Returns a list containing all of the identities for a specific AWS Account, regardless of verification status.
  * @param nextToken The token to use for pagination
  */
  def listIdentities(nextToken: Option[String] = None)(implicit region: SESRegion) =
    request[Paginated[Identity]]("ListIdentities",
      nextToken.toSeq.map("NextToken" -> _))

  /**
  * Returns the user's sending statistics.
  * @return A Seq of data points, each of which represents 15 minutes of activity.
  */
  def sendStatistics()(implicit region: SESRegion) =
    request[Seq[SendDataPoint]]("GetSendStatistics")

  /**
  * Returns the user's current sending limits.
  */
  def sendQuota()(implicit region: SESRegion) =
    request[SendQuota]("GetSendQuota")

  /**
  * Given a list of identities, returns the verification status and (for domain identities) the verification token for each identity.
  * @param identity A Seq of identities
  */
  def identityVerificationAttributes(identities: Identity*)(implicit region: SESRegion) =
    request[Seq[(Identity, VerificationAttributes)]]("GetIdentityVerificationAttributes",
      identities.zipWithIndex.map { case (id, i) =>
        s"Identities.member.${i+1}" -> id.value
      }
    )

  /**
  * Given a list of verified identities, returns a structure describing identity notification attributes.
  * @param identity A Seq of identities
  */
  def identityNotificationAttributes(identities: Identity*)(implicit region: SESRegion) =
    request[Seq[(Identity, IdentityNotificationAttributes)]]("GetIdentityNotificationAttributes",
      identities.zipWithIndex.map { case (id, i) =>
        s"Identities.member.${i+1}" -> id.value
      }
    )

  /**
  * Returns the current status of Easy DKIM signing for an entity. For domain name identities, this action also returns the DKIM tokens that are required for Easy DKIM signing, and whether Amazon SES has successfully verified that these tokens have been published.
  * @param identity A Seq of verified identities
  */
  def identityDkimAttributes(identities: Identity*)(implicit region: SESRegion) =
    request[Seq[(Identity, IdentityDkimAttributes)]]("GetIdentityDkimAttributes",
      identities.zipWithIndex.map { case (id, i) =>
        s"Identities.member.${i+1}" -> id.value
      }
    )

  /**
  * Deletes the specified identity (email address or domain) from the list of verified identities.
  * @param id The identity to be removed from the list of identities for the AWS Account.
  */
  def deleteIdentity(id: Identity)(implicit region: SESRegion) =
    request[Unit]("DeleteIdentity", Seq("Identity" -> id.value))
}
