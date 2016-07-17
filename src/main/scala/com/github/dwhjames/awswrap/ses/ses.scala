/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
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

package com.github.dwhjames.awswrap
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
      wrapAsyncMethod[DeleteIdentityRequest, DeleteIdentityResult](client.deleteIdentityAsync, deleteIdentityRequest)

  def deleteIdentity(
    identity: String
  ): Future[DeleteIdentityResult] =
    deleteIdentity(
      new DeleteIdentityRequest()
      .withIdentity(identity)
    )

  def deleteVerifiedEmailAddress(
    deleteVerifiedEmailAddressRequest: DeleteVerifiedEmailAddressRequest
  ): Future[DeleteVerifiedEmailAddressResult] =
    wrapAsyncMethod[DeleteVerifiedEmailAddressRequest, DeleteVerifiedEmailAddressResult](client.deleteVerifiedEmailAddressAsync, deleteVerifiedEmailAddressRequest)

  def deleteVerifiedEmailAddress(
    emailAddress: String
  ): Future[DeleteVerifiedEmailAddressResult] =
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
    wrapAsyncMethod[GetIdentityDkimAttributesRequest, GetIdentityDkimAttributesResult](client.getIdentityDkimAttributesAsync, getIdentityDkimAttributesRequest)

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
    wrapAsyncMethod[GetIdentityNotificationAttributesRequest, GetIdentityNotificationAttributesResult](client.getIdentityNotificationAttributesAsync, getIdentityNotificationAttributesRequest)

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
    wrapAsyncMethod[GetIdentityVerificationAttributesRequest, GetIdentityVerificationAttributesResult](client.getIdentityVerificationAttributesAsync, getIdentityVerificationAttributesRequest)

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
    wrapAsyncMethod[GetSendQuotaRequest, GetSendQuotaResult](client.getSendQuotaAsync, getSendQuotaRequest)

  def getSendQuota(): Future[GetSendQuotaResult] =
    getSendQuota(new GetSendQuotaRequest)

  def getSendStatistics(
    getSendStatisticsRequest: GetSendStatisticsRequest
  ): Future[GetSendStatisticsResult] =
    wrapAsyncMethod[GetSendStatisticsRequest, GetSendStatisticsResult](client.getSendStatisticsAsync, getSendStatisticsRequest)

  def getSendStatistics(): Future[GetSendStatisticsResult] =
    getSendStatistics(new GetSendStatisticsRequest())

  def listIdentities(
    listIdentitiesRequest: ListIdentitiesRequest
  ): Future[ListIdentitiesResult] =
    wrapAsyncMethod[ListIdentitiesRequest, ListIdentitiesResult](client.listIdentitiesAsync, listIdentitiesRequest)

  def listIdentities(): Future[ListIdentitiesResult] =
    listIdentities(new ListIdentitiesRequest)

  @deprecated("Use listIdentities", "May 15, 2012")
  def listVerifiedEmailAddresses(
    listVerifiedEmailAddressesRequest: ListVerifiedEmailAddressesRequest
  ): Future[ListVerifiedEmailAddressesResult] =
    wrapAsyncMethod[ListVerifiedEmailAddressesRequest, ListVerifiedEmailAddressesResult](client.listVerifiedEmailAddressesAsync, listVerifiedEmailAddressesRequest)

  def sendEmail(
    sendEmailRequest: SendEmailRequest
  ): Future[SendEmailResult] =
    wrapAsyncMethod[SendEmailRequest, SendEmailResult](client.sendEmailAsync, sendEmailRequest)

  def sendEmail(
    source:      String,
    destination: Destination,
    message:     Message
  ): Future[SendEmailResult] =
    sendEmail(new SendEmailRequest(source, destination, message))

  def sendRawEmail(
    sendRawEmailRequest: SendRawEmailRequest
  ): Future[SendRawEmailResult] =
    wrapAsyncMethod[SendRawEmailRequest, SendRawEmailResult](client.sendRawEmailAsync, sendRawEmailRequest)

  def sendRawEmail(
    rawMessage: RawMessage
  ): Future[SendRawEmailResult] =
    sendRawEmail(new SendRawEmailRequest(rawMessage))

  def setIdentityDkimEnabled(
    setIdentityDkimEnabledRequest: SetIdentityDkimEnabledRequest
  ): Future[SetIdentityDkimEnabledResult] =
    wrapAsyncMethod[SetIdentityDkimEnabledRequest, SetIdentityDkimEnabledResult](client.setIdentityDkimEnabledAsync, setIdentityDkimEnabledRequest)

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
    wrapAsyncMethod[SetIdentityFeedbackForwardingEnabledRequest, SetIdentityFeedbackForwardingEnabledResult](client.setIdentityFeedbackForwardingEnabledAsync, setIdentityFeedbackForwardingEnabledRequest)

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
    wrapAsyncMethod[SetIdentityNotificationTopicRequest, SetIdentityNotificationTopicResult](client.setIdentityNotificationTopicAsync, setIdentityNotificationTopicRequest)

  def shutdown(): Unit =
    client.shutdown()

  def verifyDomainDkim(
    verifyDomainDkimRequest: VerifyDomainDkimRequest
  ): Future[VerifyDomainDkimResult] =
    wrapAsyncMethod[VerifyDomainDkimRequest, VerifyDomainDkimResult](client.verifyDomainDkimAsync, verifyDomainDkimRequest)

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
    wrapAsyncMethod[VerifyDomainIdentityRequest, VerifyDomainIdentityResult](client.verifyDomainIdentityAsync, verifyDomainIdentityRequest)

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
  ): Future[VerifyEmailAddressResult] =
    wrapAsyncMethod[VerifyEmailAddressRequest, VerifyEmailAddressResult](client.verifyEmailAddressAsync, verifyEmailAddressRequest)

  def verifyEmailIdentity(
    verifyEmailIdentityRequest: VerifyEmailIdentityRequest
  ): Future[VerifyEmailIdentityResult] =
    wrapAsyncMethod[VerifyEmailIdentityRequest, VerifyEmailIdentityResult](client.verifyEmailIdentityAsync, verifyEmailIdentityRequest)
  
  def verifyEmailIdentity(
    emailAddress: String
  ): Future[VerifyEmailIdentityResult] =
    verifyEmailIdentity(
      new VerifyEmailIdentityRequest()
      .withEmailAddress(emailAddress)
    )

}
