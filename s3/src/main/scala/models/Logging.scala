package aws.s3.models

import java.util.Date

import play.api.libs.ws._

import scala.concurrent.Future
import scala.xml.Elem

import aws.core._
import aws.core.Types._
import aws.core.parsers.Parser

import aws.s3.S3._
import aws.s3.S3.HTTPMethods._
import aws.s3.S3Parsers._

import scala.concurrent.ExecutionContext.Implicits.global

import aws.s3.S3.Parameters.Permisions.Grantees._

case class LoggingStatus(bucket: String, prefix: String, grants: Seq[(Grantee, String)])
object Logging {

  import Http._

  // TODO: check that it really does works
  def enable(loggedBucket: String, targetBucket: String, grantees: Seq[Grantee] = Nil) = {
    val body =
      <BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01">
        <LoggingEnabled>
          <TargetBucket>{ targetBucket.toLowerCase }</TargetBucket>
          <TargetPrefix>{ loggedBucket.toLowerCase }-access_log-/</TargetPrefix>
          <TargetGrants>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">
                <EmailAddress>julien.tournay@pellucid.com</EmailAddress>
              </Grantee>
              <Permission>FULL_CONTROL</Permission>
            </Grant>
          </TargetGrants>
        </LoggingEnabled>
      </BucketLoggingStatus>

    request[Unit](PUT, Some(loggedBucket), body = Some(enumString(body.toString)), subresource = Some("logging"))
  }

  def get(bucketName: String) =
    request[Seq[LoggingStatus]](GET, Some(bucketName), subresource = Some("logging"))
}