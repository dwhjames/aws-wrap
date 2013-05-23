
package aws.wrap
package ses

import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConverters._

import java.util.concurrent.ExecutorService

import com.amazonaws.services.simpleemail._
import com.amazonaws.services.simpleemail.model._

class AmazonSimpleEmailServiceScalaClient(val client: AmazonSimpleEmailServiceAsyncClient) {

  def deleteIdentity(
    deleteIdentityRequest: DeleteIdentityRequest
  ): Future[DeleteIdentityResult] =
      wrapAsyncMethod(client.deleteIdentityAsync, deleteIdentityRequest)

  def deleteIdentity(
    identity: String
  ): Future[DeleteIdentityResult] =
    deleteIdentity(
      new DeleteIdentityRequest()
      .withIdentity(identity)
    )

  def deleteVerifiedEmailAddress(
    deleteVerifiedEmailAddressRequest: DeleteVerifiedEmailAddressRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.deleteVerifiedEmailAddressAsync, deleteVerifiedEmailAddressRequest)

  def deleteVerifiedEmailAddress(
    emailAddress: String
  ): Future[Unit] =
    deleteVerifiedEmailAddress(
      new DeleteVerifiedEmailAddressRequest()
      .withEmailAddress(emailAddress)
    )

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

  def getExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(client.getExecutorService())

  def getIdentityDkimAttributes(
    getIdentityDkimAttributesRequest: GetIdentityDkimAttributesRequest
  ): Future[GetIdentityDkimAttributesResult] =
    wrapAsyncMethod(client.getIdentityDkimAttributesAsync, getIdentityDkimAttributesRequest)

  def getIdentityDkimAttributes(
    identities: Iterable[String]
  ): Future[GetIdentityDkimAttributesResult] =
    getIdentityDkimAttributes(
      new GetIdentityDkimAttributesRequest()
      .withIdentities(identities.asJavaCollection)
    )

  def getIdentityNotificationAttributes(
    getIdentityNotificationAttributesRequest: GetIdentityNotificationAttributesRequest
  ): Future[GetIdentityNotificationAttributesResult] =
    wrapAsyncMethod(client.getIdentityNotificationAttributesAsync, getIdentityNotificationAttributesRequest)

  def getIdentityNotificationAttributes(
    identities: Iterable[String]
  ): Future[GetIdentityNotificationAttributesResult] =
    getIdentityNotificationAttributes(
      new GetIdentityNotificationAttributesRequest()
      .withIdentities(identities.asJavaCollection)
    )

  def getIdentityVerificationAttributes(
    getIdentityVerificationAttributesRequest: GetIdentityVerificationAttributesRequest
  ): Future[GetIdentityVerificationAttributesResult] =
    wrapAsyncMethod(client.getIdentityVerificationAttributesAsync, getIdentityVerificationAttributesRequest)

  def getIdentityVerificationAttributes(
    identities: Iterable[String]
  ): Future[GetIdentityVerificationAttributesResult] =
    getIdentityVerificationAttributes(
      new GetIdentityVerificationAttributesRequest()
      .withIdentities(identities.asJavaCollection)
    )

  def getSendQuota(
    getSendQuotaRequest: GetSendQuotaRequest
  ): Future[GetSendQuotaResult] =
    wrapAsyncMethod(client.getSendQuotaAsync, getSendQuotaRequest)

  def getSendQuota(): Future[GetSendQuotaResult] =
    getSendQuota(new GetSendQuotaRequest)

  def getSendStatistics(
    getSendStatisticsRequest: GetSendStatisticsRequest
  ): Future[GetSendStatisticsResult] =
    wrapAsyncMethod(client.getSendStatisticsAsync, getSendStatisticsRequest)

  def getSendStatistics(): Future[GetSendStatisticsResult] =
    getSendStatistics(new GetSendStatisticsRequest())

  def listIdentities(
    listIdentitiesRequest: ListIdentitiesRequest
  ): Future[ListIdentitiesResult] =
    wrapAsyncMethod(client.listIdentitiesAsync, listIdentitiesRequest)

  def listIdentities(): Future[ListIdentitiesResult] =
    listIdentities(new ListIdentitiesRequest)

  @deprecated("Use listIdentities", "May 15, 2012")
  def listVerifiedEmailAddresses(
    listVerifiedEmailAddressesRequest: ListVerifiedEmailAddressesRequest
  ): Future[ListVerifiedEmailAddressesResult] =
    wrapAsyncMethod(client.listVerifiedEmailAddressesAsync, listVerifiedEmailAddressesRequest)

  def sendEmail(
    sendEmailRequest: SendEmailRequest
  ): Future[SendEmailResult] =
    wrapAsyncMethod(client.sendEmailAsync, sendEmailRequest)

  def sendEmail(
    source:      String,
    destination: Destination,
    message:     Message
  ): Future[SendEmailResult] =
    sendEmail(new SendEmailRequest(source, destination, message))

  def sendRawEmail(
    sendRawEmailRequest: SendRawEmailRequest
  ): Future[SendRawEmailResult] =
    wrapAsyncMethod(client.sendRawEmailAsync, sendRawEmailRequest)

  def sendRawEmail(
    rawMessage: RawMessage
  ): Future[SendRawEmailResult] =
    sendRawEmail(new SendRawEmailRequest(rawMessage))

  def setIdentityDkimEnabled(
    setIdentityDkimEnabledRequest: SetIdentityDkimEnabledRequest
  ): Future[SetIdentityDkimEnabledResult] =
    wrapAsyncMethod(client.setIdentityDkimEnabledAsync, setIdentityDkimEnabledRequest)

  def setIdentityDkimEnabled(
    dkimEnabled: Boolean,
    identity:    String
  ): Future[SetIdentityDkimEnabledResult] =
    setIdentityDkimEnabled(
      new SetIdentityDkimEnabledRequest()
      .withDkimEnabled(dkimEnabled)
      .withIdentity(identity)
    )

  def setIdentityFeedbackForwardingEnabled(
    setIdentityFeedbackForwardingEnabledRequest: SetIdentityFeedbackForwardingEnabledRequest
  ): Future[SetIdentityFeedbackForwardingEnabledResult] =
    wrapAsyncMethod(client.setIdentityFeedbackForwardingEnabledAsync, setIdentityFeedbackForwardingEnabledRequest)

  def setIdentityFeedbackForwardingEnabled(
    forwardingEnabled: Boolean,
    identity:          String
  ): Future[SetIdentityFeedbackForwardingEnabledResult] =
    setIdentityFeedbackForwardingEnabled(
      new SetIdentityFeedbackForwardingEnabledRequest()
      .withForwardingEnabled(forwardingEnabled)
      .withIdentity(identity)
    )

  def setIdentityNotificationTopic(
    setIdentityNotificationTopicRequest: SetIdentityNotificationTopicRequest
  ): Future[SetIdentityNotificationTopicResult] =
    wrapAsyncMethod(client.setIdentityNotificationTopicAsync, setIdentityNotificationTopicRequest)

  def shutdown(): Unit =
    client.shutdown()

  def verifyDomainDkim(
    verifyDomainDkimRequest: VerifyDomainDkimRequest
  ): Future[VerifyDomainDkimResult] =
    wrapAsyncMethod(client.verifyDomainDkimAsync, verifyDomainDkimRequest)

  def verifyDomainDkim(
    domain: String
  ): Future[VerifyDomainDkimResult] =
    verifyDomainDkim(
      new VerifyDomainDkimRequest()
      .withDomain(domain)
    )

  def verifyDomainIdentity(
    verifyDomainIdentityRequest: VerifyDomainIdentityRequest
  ): Future[VerifyDomainIdentityResult] =
    wrapAsyncMethod(client.verifyDomainIdentityAsync, verifyDomainIdentityRequest)

  def verifyDomainIdentity(
    domain: String
  ): Future[VerifyDomainIdentityResult] =
    verifyDomainIdentity(
      new VerifyDomainIdentityRequest()
      .withDomain(domain)
    )

  @deprecated("Use verifyEmailIdentity", "May 15, 2012")
  def verifyEmailAddress(
    verifyEmailAddressRequest: VerifyEmailAddressRequest
  ): Future[Unit] =
    wrapVoidAsyncMethod(client.verifyEmailAddressAsync, verifyEmailAddressRequest)

  def verifyEmailIdentity(
    verifyEmailIdentityRequest: VerifyEmailIdentityRequest
  ): Future[VerifyEmailIdentityResult] =
    wrapAsyncMethod(client.verifyEmailIdentityAsync, verifyEmailIdentityRequest)
  
  def verifyEmailIdentity(
    emailAddress: String
  ): Future[VerifyEmailIdentityResult] =
    verifyEmailIdentity(
      new VerifyEmailIdentityRequest()
      .withEmailAddress(emailAddress)
    )

}
