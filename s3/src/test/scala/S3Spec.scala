package aws.s3

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._

import aws.core._
import org.specs2.mutable._

object S3Spec extends Specification {

  import scala.concurrent._
  import scala.concurrent.util._
  import java.util.concurrent.TimeUnit._

  implicit val region = S3Region.EU_WEST_1

  "S3 API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    def checkResult[M <: Metadata, T](r: Result[M, T]) = r match {
      case Errors(errors) => failure(errors.toString)
      case Result(_, _) => success
    }

    def del(name: String) = {
      val delete = Await.result(S3.deleteBucket(name), Duration(30, SECONDS))
      checkResult(delete)
    }

    "Create and delete a bucket" in {
      val bucketName = AWS.key + "testBucketCreate"
      val create = Await.result(S3.createBucket(bucketName), Duration(30, SECONDS))
      checkResult(create)
      del(bucketName)
    }

    "Create a private bucket using canned ACL" in {
      import aws.s3.S3.Parameters.Permisions.ACLs._
      val bucketName = AWS.key + "testBucketAcl"
      val res = Await.result(S3.createBucket(bucketName, Some(PRIVATE)), Duration(30, SECONDS))
      checkResult(res)
      del(bucketName)
    }

    "Create a private bucket with explicit permisions" in {
      import aws.s3.S3.Parameters.Permisions._
      import Grantees._
      val bucketName = AWS.key + "testBucketPerms"

      val perms =
        GRANT_READ(Email("erwan.loisant@pellucid.com"), Email("dustin.whitney@pellucid.com")) ::
        GRANT_WRITE(Email("erwan.loisant@pellucid.com")) ::
        GRANT_READ_ACP(Email("erwan.loisant@pellucid.com")) :: Nil

      val res = Await.result(S3.createBucket(bucketName, permissions = perms), Duration(30, SECONDS))
      checkResult(res)
      del(bucketName)
    }
  }
}