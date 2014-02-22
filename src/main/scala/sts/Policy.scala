package com.pellucid.wrap
package sts

import org.joda.time.DateTime

import scala.collection.JavaConverters._

import com.amazonaws.auth.{policy => ap}
import com.amazonaws.auth.policy.Statement.Effect
import com.amazonaws.services.securitytoken.{model => am}

case class Action(
    name: String
) {

  val aws = new ap.Action {
    override def getActionName: String = name
  }

}

case class Resource(
    id: String
) {

  val aws = new ap.Resource(id)

}

case class Condition(
    key:      String,
    typeName: String,
    values:   Seq[String]
) {

  val aws = new ap.Condition()
  .withConditionKey(key)
  .withType(typeName)
  .withValues(values.asJava)

}

case class Statement(
    effect:     Effect,
    actions:    Seq[Action],
    resources:  Seq[Resource],
    id:         Option[String] = None,
    conditions: Seq[Condition] = Nil,
    principals: Seq[ap.Principal] = Nil
) {

  val aws = new ap.Statement(effect)
  .withActions(actions.map(_.aws): _*)
  .withResources(resources.map(_.aws): _*)
  .withConditions(conditions.map(_.aws): _*)
  .withPrincipals(principals: _*)

  id.foreach(aws.setId)

}

case class Policy(
    statements: Seq[Statement],
    id:         Option[String] = None
) {

  val aws = new ap.Policy()
  .withStatements(statements.map(_.aws): _*)

  id.foreach(aws.setId)

  def toJson = aws.toJson

}

case class TemporaryCredentials(
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: String,
    expiration: DateTime
) {

  val aws = new am.Credentials(accessKeyId, secretAccessKey, sessionToken, expiration.toDate)

}

object TemporaryCredentials {
  def apply(
    c: am.Credentials
  ): TemporaryCredentials =
    new TemporaryCredentials(
      c.getAccessKeyId,
      c.getSecretAccessKey,
      c.getSessionToken,
      new DateTime(c.getExpiration)
    )
}

case class SessionToken(credentials: TemporaryCredentials)

case class FederatedUser(
    userId: String,
    arn:    String
) {
  val aws = new am.FederatedUser(userId, arn)
}

object FederatedUser {
  def apply(
    u: am.FederatedUser
  ): FederatedUser = {
    new FederatedUser(
      u.getFederatedUserId,
      u.getArn
    )
  }
}

case class FederationToken(
    user:        FederatedUser,
    credentials: TemporaryCredentials
)