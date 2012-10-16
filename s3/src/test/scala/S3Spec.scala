package aws.s3

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._

import aws.core._
import aws.s3.models._
import aws.s3.S3.Parameters.Permisions._
import org.specs2.mutable._

import scala.concurrent._
import scala.concurrent.util._
import java.util.concurrent.TimeUnit._

import scala.concurrent.ExecutionContext.Implicits.global

object TestUtils extends Specification { // Evil hack to access Failure
  implicit val region = S3Region.EU_WEST_1

  def checkResult[M <: Metadata, T](r: Result[M, T]) = r match {
    case Errors(errors) => failure(errors.toString)
    case Result(_, _) => success
  }

  def del(name: String) = {
    val delete = Await.result(Bucket.delete(name), Duration(30, SECONDS))
    checkResult(delete)
  }
}

import TestUtils._

object BucketSpec extends Specification {

  "S3 Bucket API" should {

    "Create and delete a bucket" in {
      val bucketName = AWS.key + "testBucketCreate"
      val create = Await.result(Bucket.create(bucketName), Duration(30, SECONDS))
      checkResult(create)
      del(bucketName)
    }

    "Create a private bucket using canned ACL" in {
      import aws.s3.S3.Parameters.Permisions.ACLs._
      val bucketName = AWS.key + "testBucketAcl"
      val res = Await.result(Bucket.create(bucketName, Some(PRIVATE)), Duration(30, SECONDS))
      checkResult(res)
      del(bucketName)
    }

    "Create a private bucket with explicit permisions" in {
      import Grantees._
      val bucketName = AWS.key + "testBucketPerms"
      val perms =
        GRANT_READ(Email("erwan.loisant@pellucid.com"), Email("dustin.whitney@pellucid.com")) ::
        GRANT_WRITE(Email("erwan.loisant@pellucid.com")) ::
        GRANT_READ_ACP(Email("erwan.loisant@pellucid.com")) :: Nil

      val res = Await.result(Bucket.create(bucketName, permissions = perms), Duration(30, SECONDS))
      checkResult(res)
      del(bucketName)
    }

    "List buckets" in {
      val res = Await.result(Bucket.list(), Duration(30, SECONDS))
      checkResult(res)
    }
  }
}

object LoggingSpec extends Specification {
  "S3 Bucket Logging API" should {

    "Enable Logging" in {
      import Grantees._
      val ps =
        GRANT_WRITE(Uri("http://acs.amazonaws.com/groups/s3/LogDelivery")) ::
        GRANT_READ_ACP(Uri("http://acs.amazonaws.com/groups/s3/LogDelivery")) :: Nil

      val target = AWS.key + "testBucketLoggingTarget"
      val t = Await.result(Bucket.create(target, permissions = ps), Duration(30, SECONDS))

      val logged = AWS.key + "testBucketLogging"
      Await.result(Bucket.create(logged), Duration(30, SECONDS))

      val res = Await.result(Logging.enable(logged, target), Duration(30, SECONDS))

      del(logged)
      del(target)

      checkResult(res)
    }

    "Show Logging Statuses" in {
      val bucketName = AWS.key + "testBucketLoggingStatuses"
      Await.result(Bucket.create(bucketName), Duration(30, SECONDS))
      val res = Await.result(Logging.get(bucketName), Duration(30, SECONDS))
      checkResult(res)
      del(bucketName)
    }

  }
}

object TagSpec extends Specification {
  "S3 Bucket Tagging API" should {
    "create tags" in {
      val tagged = AWS.key + "testBucketLoggingTagged"
      val c = Await.result(Bucket.create(tagged), Duration(30, SECONDS))
      val res = Await.result(Tag.create(tagged, Tag("Project", "Project One"), Tag("User", "jsmith")), Duration(30, SECONDS))
      del(tagged)
      checkResult(res)
    }
  }
}