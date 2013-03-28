package aws.s3

import scala.concurrent._
import scala.concurrent.duration._

import aws.core._
import aws.s3.models._

import org.specs2.mutable._


object TestUtils extends Specification { // Evil hack to access Failure
  // implicit val loc = LocationConstraint.EU_WEST_1

  def checkResult[M <: Metadata, T](r: Result[M, T]) = r match {
    case AWSError(_, _, message) => failure(message)
    case Result(_, _) => success
  }

  def waitFor[T](f: Future[T]) = Await.result(f, Duration(30, SECONDS))

  def del(name: String) = {
    val delete = waitFor(S3.Bucket.delete(name))
    checkResult(delete)
  }
}

import TestUtils._

object BucketSpec extends Specification {

  "S3 Bucket API" should {

    "Create and delete a bucket" in {
      val bucketName = S3.s3AwsKey + "testBucketCreate"
      val create = waitFor(S3.Bucket.create(bucketName))
      checkResult(create)
      del(bucketName)
    }

    "Create a private bucket using canned ACL" in {
      val bucketName = S3.s3AwsKey + "testBucketAcl"
      val res = waitFor(S3.Bucket.create(bucketName, Some(CannedACL.PRIVATE)))
      checkResult(res)
      del(bucketName)
    }

    "Create a private bucket with explicit permisions" in {
      import Permissions._
      val bucketName = S3.s3AwsKey + "testBucketPerms"
      val perms =
        GRANT_READ(Email("erwan.loisant@pellucid.com"), Email("dustin.whitney@pellucid.com")) ::
        GRANT_WRITE(Email("erwan.loisant@pellucid.com")) ::
        GRANT_READ_ACP(Email("erwan.loisant@pellucid.com")) :: Nil

      val res = waitFor(S3.Bucket.create(bucketName, permissions = perms))
      checkResult(res)
      del(bucketName)
    }

    "List buckets" in {
      val res = waitFor(S3.Bucket.list())
      checkResult(res)
    }

    "Enable versionning" in {

      val bucketName = S3.s3AwsKey + "testBucketEnableVersion"
      val cr = waitFor(S3.Bucket.create(bucketName))

      val res = waitFor(S3.Bucket.setVersioningConfiguration(bucketName, VersionState.ENABLED))

      checkResult(res)
      del(bucketName)
    }

    "Enable versionning and MFA" in {
      skipped("Needs an MFA device")

      val bucketName = S3.s3AwsKey + "testBucketEnableMFA"
      val cr = waitFor(S3.Bucket.create(bucketName))

      val res = waitFor(S3.Bucket.setVersioningConfiguration(bucketName, VersionState.ENABLED, Some(MFADeleteState.ENABLED -> MFA(???, ???))))

      checkResult(res)
      del(bucketName)
    }
  }
}

object LoggingSpec extends Specification {
  "S3 Bucket Logging API" should {

    "Enable Logging" in {
      import Permissions._
      val ps =
        GRANT_WRITE(Uri("http://acs.amazonaws.com/groups/s3/LogDelivery")) ::
        GRANT_READ_ACP(Uri("http://acs.amazonaws.com/groups/s3/LogDelivery")) :: Nil

      val target = S3.s3AwsKey + "testBucketLoggingTarget"
      val t = waitFor(S3.Bucket.create(target, permissions = ps))

      val logged = S3.s3AwsKey + "testBucketLogging"
      waitFor(S3.Bucket.create(logged))

      val res = waitFor(S3.Logging.enable(logged, target, Seq(Email("dustin.whitney@pellucid.com") -> LoggingPermission.FULL_CONTROL)))

      del(logged)
      del(target)

      checkResult(res)
    }

    "Show Logging Statuses" in {
      val bucketName = S3.s3AwsKey + "testBucketLoggingStatuses"
      waitFor(S3.Bucket.create(bucketName))
      val res = waitFor(S3.Logging.get(bucketName))
      checkResult(res)
      del(bucketName)
    }

  }
}

object TagSpec extends Specification {
  "S3 Bucket Tagging API" should {
    "create tags" in {
      val tagged = S3.s3AwsKey + "testBucketLoggingTagged"
      val c = waitFor(S3.Bucket.create(tagged))
      val res = waitFor(S3.Tag.create(tagged, Tag("Project", "Project One"), Tag("User", "jsmith")))
      del(tagged)
      checkResult(res)
    }

    "list tags" in {
      val tagged = S3.s3AwsKey + "testBucketLoggingTaggedList"
      val c = waitFor(S3.Bucket.create(tagged))
      val tags = Seq(Tag("Project", "Project One"), Tag("User", "jsmith"))
      waitFor(S3.Tag.create(tagged, tags: _*))

      val res = waitFor(S3.Tag.get(tagged))

      del(tagged)

      checkResult(res)
      res.body must haveSize(2)
      res.body must containAllOf(tags)
    }

    "delete tags" in {
      val tagged = S3.s3AwsKey + "testBucketLoggingTaggedDelete"
      val c = waitFor(S3.Bucket.create(tagged))
      val tags = Seq(Tag("Project", "Project One"), Tag("User", "jsmith"))
      waitFor(S3.Tag.create(tagged, tags: _*))
      val res = waitFor(S3.Tag.delete(tagged))
      // get returns "AWSError(NoSuchTagSet,The TagSet does not exist)"
      //val res = Await.result(S3.Tag.get(tagged), Duration(30, SECONDS))
      del(tagged)
      checkResult(res)
    }
  }
}

object CORSSpec extends Specification {

  import HttpMethod._

  "S3 Bucket CORS API" should {
    "create cors" in {
      val cors = S3.s3AwsKey + "testBucketCors"
      val cr = waitFor(S3.Bucket.create(cors))

      val rules = Seq(
        CORSRule(
          origins = Seq("http://*.zenexity.com"),
          methods = PUT :: POST :: GET :: Nil))

      val res = waitFor(S3.CORSRule.create(cors, rules: _*))

      del(cors)
      checkResult(res)
    }

    "get cors" in {
      val cors = S3.s3AwsKey + "testBucketCorsGet"
      val cr = waitFor(S3.Bucket.create(cors))

      val rules = Seq(
        CORSRule(
          origins = Seq("http://*.zenexity.com"),
          methods = PUT :: POST :: GET :: Nil))

      waitFor(S3.CORSRule.create(cors, rules: _*))

      var res = waitFor(S3.CORSRule.get(cors))
      del(cors)
      checkResult(res)

      res.body must haveSize(1)
      res.body must containAllOf(rules)
    }

    "delete cors" in {
      val cors = S3.s3AwsKey + "testBucketCorsDelete"
      val cr = waitFor(S3.Bucket.create(cors))

      val rules = Seq(
        CORSRule(
          origins = Seq("http://*.zenexity.com"),
          methods = PUT :: POST :: GET :: Nil))

      waitFor(S3.CORSRule.create(cors, rules: _*))
      waitFor(S3.CORSRule.get(cors))
      val res = waitFor(S3.CORSRule.delete(cors))

      del(cors)
      checkResult(res)
    }

  }
}

object LifecycleSpec extends Specification {

  "S3 Bucket lifecycle API" should {
    "create lifecycle" in {
      val bucketName = S3.s3AwsKey + "testBucketLifecycle"

      val cr = waitFor(S3.Bucket.create(bucketName))
      val res = waitFor(S3.Lifecycle.create(bucketName, LifecycleConf(Some("42"), "test-", LifecycleStatus.ENABLED, Duration(42, DAYS))))

      del(bucketName)
      checkResult(res)
    }

    "delete lifecycle" in {
      val bucketName = S3.s3AwsKey + "testBucketLifecycleDelete"

      val cr = waitFor(S3.Bucket.create(bucketName))
      waitFor(S3.Lifecycle.create(bucketName, LifecycleConf(Some("42"), "test-", LifecycleStatus.ENABLED, Duration(42, DAYS))))
      val res = waitFor(S3.Lifecycle.delete(bucketName))

      del(bucketName)
      checkResult(res)
    }

    "get lifecycle" in {
      val bucketName = S3.s3AwsKey + "testBucketLifecycleGet"

      val cr = waitFor(S3.Bucket.create(bucketName))
      val conf = LifecycleConf(Some("42"), "test-", LifecycleStatus.ENABLED, Duration(42, DAYS))
      waitFor(S3.Lifecycle.create(bucketName, conf))
      val res = waitFor(S3.Lifecycle.get(bucketName))

      del(bucketName)
      checkResult(res)
      res.body must_== Seq(conf)
    }
  }
}

object S3ObjectSpec extends Specification {

  "S3 Object API" should {

    "get versions" in {
      val bucketName = S3.s3AwsKey + "testBucketVersions"
      val cr = waitFor(S3.Bucket.create(bucketName))

      // TODO: put objects to get versions
      val res = waitFor(S3.S3Object.getVersions(bucketName))

      del(bucketName)
      checkResult(res)
    }

    "get contents" in {
      val bucketName = S3.s3AwsKey + "testBucketContent"
      val cr = waitFor(S3.Bucket.create(bucketName))

      // TODO: put objects to get versions
      val res = waitFor(S3.S3Object.content(bucketName))
      del(bucketName)
      checkResult(res)
    }

    "upload & delete objects" in {

      val bucketName = S3.s3AwsKey + "testBucketUp"
      val cr = waitFor(S3.Bucket.create(bucketName))

      val f = new java.io.File("s3/src/test/resources/fry.gif")
      if(!f.exists)
        skipped(s"File not found: $f")

      val resUp = waitFor(S3.S3Object.put(bucketName, f))
      val resDel = waitFor(S3.S3Object.delete(bucketName, f.getName))
      del(bucketName)
      checkResult(resUp)
      checkResult(resDel)
    }

    "delete a specific version" in {

      val bucketName = S3.s3AwsKey + "testBucketDelVersions"
      val cr = waitFor(S3.Bucket.create(bucketName))

      val vr = waitFor(S3.Bucket.setVersioningConfiguration(bucketName, VersionState.ENABLED))

      val f = new java.io.File("s3/src/test/resources/fry.gif")
      if(!f.exists)
        skipped(s"File not found: $f")

      val resUp = waitFor(S3.S3Object.put(bucketName, f))
      var version = resUp.metadata.versionId

      val resDel = waitFor(S3.S3Object.delete(bucketName, f.getName, version))

      // cleanup
      val versions = waitFor(S3.S3Object.getVersions(bucketName))
      val ids = for(v <- versions.body.versions;
        id <- v.id
      ) yield f.getName -> Some(id)
      waitFor(S3.S3Object.batchDelete(bucketName, ids))

      del(bucketName)
      checkResult(resDel)
      resDel.metadata.versionId must_== resUp.metadata.versionId
    }

    "batch delete objects" in {
      import scala.language.postfixOps

      val bucketName = S3.s3AwsKey + "testBucketDel"
      val cr = waitFor(S3.Bucket.create(bucketName))

      val f = new java.io.File("s3/src/test/resources/fry.gif")
      val f2 = new java.io.File("s3/src/test/resources/fry2.jpg")

      if(!f.exists)
        skipped(s"File not found: $f")
      if(!f2.exists)
        skipped(s"File not found: $f2")

      val resUp1 = waitFor(S3.S3Object.put(bucketName, f))
      val resUp2 = waitFor(S3.S3Object.put(bucketName, f2))

      val resDel = waitFor(S3.S3Object.batchDelete(bucketName, Seq(f.getName -> None, f2.getName -> None)))

      val content = waitFor(S3.S3Object.content(bucketName))


      del(bucketName)

      checkResult(resUp1)
      checkResult(resUp2)
      checkResult(resDel)

      content.body.contents must be empty
    }

  }
}

object PolicySpec extends Specification {

  "S3 Policy API" should {

    import PolicyCondition.Key

    def policy(bucketName: String) = Policy(
      id = Some("aaaa-bbbb-cccc-dddd"),
      statements = Seq(
        PolicyStatement(
          effect = PolicyEffect.ALLOW,
          sid = Some("1"),
          principal = Some("AWS" -> Seq("*")),
          action = Seq("s3:GetObject*"),
          conditions = Seq(
            PolicyConditions.Strings.Equals(Key.USER_AGENT -> Seq("PAF")),
            PolicyConditions.Exists(Key.KeyFor(Key.REFERER) -> Seq(true))),
          resource = Seq("arn:aws:s3:::%s/*".format(bucketName.toLowerCase)))))

    "create policy" in {
      val bucketName = S3.s3AwsKey + "testBucketPoliciesCreate"
      val cr = waitFor(S3.Bucket.create(bucketName))
      val res = waitFor(S3.Policy.create(bucketName, policy(bucketName)))

      del(bucketName)
      checkResult(res)
    }

    "get policy" in {
      val bucketName = S3.s3AwsKey + "testBucketPoliciesGet"
      val cr = waitFor(S3.Bucket.create(bucketName))
      waitFor(S3.Policy.create(bucketName, policy(bucketName)))

      val res = waitFor(S3.Policy.get(bucketName))

      del(bucketName)
      checkResult(res)

      // XXX: toString == Evil workaround
      res.body.toString must_== policy(bucketName).toString
    }

    "delete policy" in {
      val bucketName = S3.s3AwsKey + "testBucketPoliciesDelete"
      val cr = waitFor(S3.Bucket.create(bucketName))
      waitFor(S3.Policy.create(bucketName, policy(bucketName)))
      val res = waitFor(S3.Policy.delete(bucketName))
      del(bucketName)
      checkResult(res)
    }

  }
}


object NotificationsSpec extends Specification {
  "S3 Notification API" should {

    "create notification conf" in {
      skipped("Needs a topic to be created")
      val bucketName = S3.s3AwsKey + "testBucketNotifCreate"
      val cr = waitFor(S3.Bucket.create(bucketName))
      val res = waitFor(S3.Notification.create(bucketName, NotificationConfiguration(s"arn:aws:sns:us-east-1:123456789012:myTopic", NotificationEvent.REDUCED_REDUNDANCY_LOST_OBJECT)))

      del(bucketName)
      checkResult(res)
    }

    "get notification conf" in {
      val bucketName = S3.s3AwsKey + "testBucketNotifGet"
      val cr = waitFor(S3.Bucket.create(bucketName))

      val res = waitFor(S3.Notification.get(bucketName))

      del(bucketName)
      checkResult(res)
    }
  }
}
