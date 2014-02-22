package com.pellucid.wrap
package sts

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.Duration

import java.util.concurrent.ExecutorService

import com.amazonaws.services.securitytoken._
import com.amazonaws.services.securitytoken.model._

class AWSSecurityTokenServiceScalaClient(
    val client: AWSSecurityTokenServiceAsyncClient,
    implicit val execCtx: ExecutionContext
) {

  def getFederationToken(
    federationTokenRequest: GetFederationTokenRequest
  ): Future[FederationToken] =
    wrapAsyncMethod(client.getFederationTokenAsync, federationTokenRequest).map { result =>
      FederationToken(
        user = FederatedUser(result.getFederatedUser),
        credentials = TemporaryCredentials(result.getCredentials)
      )
    }

  def getFederationToken(
    name:     String,
    policy:   Policy,
    duration: Duration
  ): Future[FederationToken] =
    getFederationToken(
      new GetFederationTokenRequest(name)
      .withPolicy(policy.toJson)
      .withDurationSeconds(duration.toSeconds.toInt)
    )

  def getSessionToken(
    sessionTokenRequest: GetSessionTokenRequest
  ): Future[SessionToken] =
    wrapAsyncMethod(client.getSessionTokenAsync, sessionTokenRequest).map { result =>
      SessionToken(
        TemporaryCredentials(result.getCredentials)
      )
    }

  def getSessionToken(
    serialNumber: String,
    tokenCode:    String,
    duration:     Duration
  ): Future[SessionToken] =
    getSessionToken(
      new GetSessionTokenRequest()
      .withSerialNumber(serialNumber)
      .withTokenCode(tokenCode)
      .withDurationSeconds(duration.toSeconds.toInt)
    )

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

}
