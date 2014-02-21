package com.pellucid.wrap
package sqs

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
  ): Future[GetFederationTokenResult] =
    wrapAsyncMethod(client.getFederationTokenAsync, federationTokenRequest)

  def getFederationToken(
    name: String,
    policy: Policy,
    duration: Duration
  ): Future[GetFederationTokenResult] =
    getFederationToken(
      new GetFederationTokenRequest(name)
        .withPolicy(policy.toJson)
        .withDurationSeconds(duration.toSeconds.toInt)
    )

  def getSessionToken(
    sessionTokenRequest: GetSessionTokenRequest
  ): Future[GetSessionTokenResult] =
    wrapAsyncMethod(client.getSessionTokenAsync, sessionTokenRequest)

  def getSessionToken(
    serialNumber: String,
    tokenCode: String,
    duration: Duration
  ): Future[GetSessionTokenResult] =
    getSessionToken(
      new GetSessionTokenRequest()
      .withSerialNumber(serialNumber)
      .withTokenCode(tokenCode)
      .withDurationSeconds(duration.toSeconds.toInt)
    )

  def getExecutorService(): ExecutorService =
    client.getExecutorService()

}
