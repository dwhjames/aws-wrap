
package scratch

import com.pellucid.wrap.dynamodb._

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import java.util.UUID

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat


case class Forum(
  name:     String,
  category: String,
  threads:  Long,
  messages: Long,
  views:    Long
)

object Forum {

  val tableName = "Forum"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(Forum.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(Schema.stringAttribute(Attributes.name))
    .withKeySchema(Schema.hashKey(Attributes.name))

  object Attributes {
    val name     = "Name"
    val category = "Category"
    val threads  = "Threads"
    val messages = "Messages"
    val views    = "Views"
  }

  implicit object forumSerializer extends DynamoDBSerializer[Forum] {
    override val tableName = Forum.tableName
    override val hashAttributeName = Attributes.name

    override def primaryKeyOf(forum: Forum) =
      Map(Attributes.name -> forum.name)

    override def toAttributeMap(forum: Forum) =
      Map(
        Attributes.name     -> forum.name,
        Attributes.category -> forum.category,
        Attributes.threads  -> forum.threads,
        Attributes.messages -> forum.messages,
        Attributes.views    -> forum.views
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      Forum(
        name     = item(Attributes.name),
        category = item(Attributes.category),
        threads  = item(Attributes.threads),
        messages = item(Attributes.messages),
        views    = item(Attributes.views)
      )
  }
}

case class ForumThread(
  forumName:          String,
  subject:            String,
  message:            String,
  lastPostedBy:       String,
  lastPostedDateTime: DateTime,
  views:              Long,
  replies:            Long,
  answered:           Long,
  tags:               Set[String]
)

object ForumThread {

  val tableName = "Thread"
  val secondaryIndexName = "LastPostedIndex"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(ForumThread.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(
      Schema.stringAttribute(Attributes.forumName),
      Schema.stringAttribute(Attributes.subject),
      Schema.stringAttribute(Attributes.lastPostedDateTime)
    )
    .withKeySchema(
      Schema.hashKey(Attributes.forumName),
      Schema.rangeKey(Attributes.subject)
    )
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
      .withIndexName(ForumThread.secondaryIndexName)
      .withKeySchema(
        Schema.hashKey(Attributes.forumName),
        Schema.rangeKey(Attributes.lastPostedDateTime)
      )
      .withProjection(
        new Projection()
        .withProjectionType(ProjectionType.KEYS_ONLY)
      )
    )

  object Attributes {
    val forumName          = "ForumName"
    val subject            = "Subject"
    val message            = "Message"
    val lastPostedBy       = "LastPostedBy"
    val lastPostedDateTime = "LastPostedDateTime"
    val views              = "Views"
    val replies            = "Replies"
    val answered           = "Answered"
    val tags               = "Tags"
  }

  implicit object forumThreadSerializer extends DynamoDBSerializer[ForumThread] {
    private val fmt = ISODateTimeFormat.dateTime

    override val tableName = ForumThread.tableName
    override val hashAttributeName = Attributes.forumName
    override val rangeAttributeName = Some(Attributes.subject)

    override def primaryKeyOf(thread: ForumThread) =
      Map(
        Attributes.forumName -> thread.forumName,
        Attributes.subject   -> thread.subject
      )

    override def toAttributeMap(thread: ForumThread) =
      Map(
        Attributes.forumName          -> thread.forumName,
        Attributes.subject            -> thread.subject,
        Attributes.message            -> thread.message,
        Attributes.lastPostedBy       -> thread.lastPostedBy,
        Attributes.lastPostedDateTime -> fmt.print(thread.lastPostedDateTime),
        Attributes.views              -> thread.views,
        Attributes.replies            -> thread.replies,
        Attributes.answered           -> thread.answered,
        Attributes.tags               -> thread.tags
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      ForumThread(
        forumName          = item(Attributes.forumName),
        subject            = item(Attributes.subject),
        message            = item(Attributes.message),
        lastPostedBy       = item(Attributes.lastPostedBy),
        lastPostedDateTime = fmt.parseDateTime(item(Attributes.lastPostedDateTime)),
        views              = item(Attributes.views),
        replies            = item(Attributes.replies),
        answered           = item(Attributes.answered),
        tags               = item(Attributes.tags)
      )
  }
}

case class Reply(
  id:            String,
  replyDateTime: DateTime,
  message:       String,
  postedBy:      String
)

object Reply {

  val tableName = "Reply"
  val secondaryIndexName = "PostedByIndex"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(Reply.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(
      Schema.stringAttribute(Attributes.id),
      Schema.stringAttribute(Attributes.replyDateTime),
      Schema.stringAttribute(Attributes.postedBy)
    )
    .withKeySchema(
      Schema.hashKey(Attributes.id),
      Schema.rangeKey(Attributes.replyDateTime)
    )
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
      .withIndexName(Reply.secondaryIndexName)
      .withKeySchema(
        Schema.hashKey(Attributes.id),
        Schema.rangeKey(Attributes.postedBy)
      )
      .withProjection(
        new Projection()
        .withProjectionType(ProjectionType.KEYS_ONLY)
      )
    )

  object Attributes {
    val id            = "Id"
    val replyDateTime = "ReplyDateTime"
    val message       = "Message"
    val postedBy      = "PostedBy"
  }

  implicit object replySerializer extends DynamoDBSerializer[Reply] {
    private val fmt = ISODateTimeFormat.dateTime

    override val tableName = Reply.tableName
    override val hashAttributeName = Attributes.id
    override val rangeAttributeName = Some(Attributes.replyDateTime)

    override def primaryKeyOf(reply: Reply) =
      Map(
        Attributes.id            -> reply.id,
        Attributes.replyDateTime -> fmt.print(reply.replyDateTime)
      )

    override def toAttributeMap(reply: Reply) =
      Map(
        Attributes.id            -> reply.id,
        Attributes.replyDateTime -> fmt.print(reply.replyDateTime),
        Attributes.message       -> reply.message,
        Attributes.postedBy      -> reply.postedBy
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      Reply(
        id            = item(Attributes.id),
        replyDateTime = fmt.parseDateTime(item(Attributes.replyDateTime)),
        message       = item(Attributes.message),
        postedBy      = item(Attributes.postedBy)
      )
  }
}

object Scratch {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val t0 = DateTime.now.minusDays(1)
  val t1 = DateTime.now.minusDays(7)
  val t2 = DateTime.now.minusDays(14)
  val t3 = DateTime.now.minusDays(21)

  val sampleForums: Seq[Forum] = Seq(
      Forum(
        name     = "Amazon DynamoDB",
        category = "Amazon Web Services",
        threads  = 2,
        messages = 3,
        views    = 1000
      ),
      Forum(
        name     = "Amazon S3",
        category = "Amazon Web Services",
        threads  = 1,
        messages = 1,
        views    = 500
      )
    )

  val sampleThreads: Seq[ForumThread] = Seq(
      ForumThread(
        forumName          = "Amazon DynamoDB",
        subject            = "DynamoDB Thread 1",
        message            = "DynamoDB thread 1 message",
        lastPostedBy       = "User A",
        lastPostedDateTime = t2,
        views              = 0,
        replies            = 0,
        answered           = 0,
        tags               = Set("index", "primarykey", "table")
      ),
      ForumThread(
        forumName          = "Amazon DynamoDB",
        subject            = "DynamoDB Thread 2",
        message            = "DynamoDB thread 2 message",
        lastPostedBy       = "User A",
        lastPostedDateTime = t3,
        views              = 0,
        replies            = 0,
        answered           = 0,
        tags               = Set("index", "primarykey", "rangekey")
      ),
      ForumThread(
        forumName          = "Amazon S3",
        subject            = "S3 Thread 1",
        message            = "S3 thread 1 message",
        lastPostedBy       = "User A",
        lastPostedDateTime = t1,
        views              = 0,
        replies            = 0,
        answered           = 0,
        tags               = Set("largeobjects", "multipart upload")
      )
    )

  val sampleReplies: Seq[Reply] = Seq(
      Reply(
        id            = "Amazon DynamoDB#DynamoDB Thread 1",
        replyDateTime = t3,
        message       = "DynamoDB Thread 1 Reply 1 text",
        postedBy      = "User A"
      ),
      Reply(
        id            = "Amazon DynamoDB#DynamoDB Thread 1",
        replyDateTime = t2,
        message       = "DynamoDB Thread 1 Reply 2 text",
        postedBy      = "User B"
      ),
      Reply(
        id            = "Amazon DynamoDB#DynamoDB Thread 2",
        replyDateTime = t1,
        message       = "DynamoDB Thread 2 Reply 1 text",
        postedBy      = "User A"
      ),
      Reply(
        id            = "Amazon DynamoDB#DynamoDB Thread 2",
        replyDateTime = t0,
        message       = "DynamoDB Thread 2 Reply 2 text",
        postedBy      = "User A"
      )
    )

  def main(args: Array[String]) {

    val credentials = new PropertiesCredentials(
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("credentials.properties")
      )

    val client = new AmazonDynamoDBScalaClient(new AmazonDynamoDBAsyncClient(credentials))

    def awaitTableCreation(tableName: String): TableDescription = {
      logger.info(s"Waiting for $tableName table to become active.")

      val deadline = 10.minutes.fromNow

      while (deadline.hasTimeLeft) {
        val result = Await.result(
          client.describeTable(tableName),
          10.seconds
        )
        val description = result.getTable
        if (description.getTableStatus == TableStatus.ACTIVE.toString)
          return description

        Thread.sleep(20 * 1000)
      }
      throw new RuntimeException(s"Timed out waiting for $tableName table to become active.")
    }

    def tryDeleteTable(tableName: String) {
      logger.info(s"Deleting $tableName table")
      Await.result(
        client.deleteTable(tableName) recover { case e: ResourceNotFoundException => () },
        10.seconds
      )
    }

    def awaitTableDeletion(tableName: String) {
      logger.info(s"Waiting for $tableName table to be deleted.")

      val deadline = 10.minutes.fromNow

      while (deadline.hasTimeLeft) {
        try {
          val result = Await.result(
            client.describeTable(tableName),
            10.seconds
          )
          if (result.getTable.getTableStatus == TableStatus.ACTIVE.toString) return ()
          Thread.sleep(20 * 1000)
        } catch {
          case e: ResourceNotFoundException =>
            return ()
        }
      }
      throw new RuntimeException(s"Timed out waiting for $tableName table to be deleted.")
    }

    tryDeleteTable("Forum")
    tryDeleteTable("Thread")
    tryDeleteTable("Reply")

    awaitTableDeletion("Forum")
    awaitTableDeletion("Thread")
    awaitTableDeletion("Reply")

    logger.info("Creating Forum table")
    Await.result(
      client.createTable(Forum.tableRequest),
      10.seconds
    )

    logger.info("Creating Thread table")
    Await.result(
      client.createTable(ForumThread.tableRequest),
      10.seconds
    )

    println(awaitTableCreation("Forum"))

    println(awaitTableCreation("Thread"))

    logger.info("Creating Reply table")
    Await.result(
      client.createTable(Reply.tableRequest),
      10.seconds
    )

    println(awaitTableCreation("Reply"))


    val mapper = AmazonDynamoDBScalaMapper(client)


    logger.info("Uploading sample Forum items")
    Await.result(
      Future.sequence {
        sampleForums map { mapper.dump(_) }
      },
      10.seconds
    )

    logger.info("Uploading sample Thread items")
    Await.result(
      Future.sequence {
        sampleThreads map { mapper.dump(_) }
      },
      10.seconds
    )

    logger.info("Uploading sample Reply items")
    Await.result(
      Future.sequence {
        sampleReplies map { mapper.dump(_) }
      },
      10.seconds
    )


    logger.info("Count all three tables")
    Await.result(
      for {
        forumCount  <- mapper.countScan[Forum]()
        threadCount <- mapper.countScan[ForumThread]()
        replyCount  <- mapper.countScan[Reply]()
      } yield {
        assert {
          forumCount == sampleForums.size
        }
        assert {
          threadCount == sampleThreads.size
        }
        assert {
          replyCount == sampleReplies.size
        }
      },
      10.seconds
    )

    logger.info("Scan all three tables")
    Await.result(
      for {
        forums  <- mapper.scan[Forum](Map("Category" -> ScanCondition.contains("Services")))
        threads <- mapper.scan[ForumThread](Map("Tags" -> ScanCondition.contains("index")))
        replies <- mapper.scan[Reply](Map("PostedBy" -> ScanCondition.in("User A", "User B")))
      } yield {
        assert {
          forums.size == sampleForums.size
        }
        assert {
          threads forall (_.forumName == "Amazon DynamoDB")
        }
        assert {
          replies.size == sampleReplies.size
        }
      },
      10.seconds
    )

    logger.info("Query threads table")
    Await.result(
      for {
        res1 <- mapper.query[ForumThread]("Amazon S3")
        c1   <- mapper.countQuery[ForumThread]("Amazon S3")
        res2 <- mapper.query[ForumThread]("Amazon S3", QueryCondition.beginsWith("S3"))
        c2   <- mapper.countQuery[ForumThread]("Amazon S3", QueryCondition.beginsWith("S3"))
        res3 <- mapper.query[ForumThread](
                  ForumThread.secondaryIndexName,
                  "Amazon S3",
                  ForumThread.Attributes.lastPostedDateTime,
                  QueryCondition.greaterThan("2000-01-01")
                )
        c3   <- mapper.countQuery[ForumThread](
                  ForumThread.secondaryIndexName,
                  "Amazon S3",
                  ForumThread.Attributes.lastPostedDateTime,
                  QueryCondition.greaterThan("2000-01-01")
                )
      } yield {
        assert {
          res1 == res2
        }
        assert {
          res2 == res3
        }
        assert {
          c1 == 1
        }
        assert {
          c2 == 1
        }
        assert {
          c3 == 1
        }
      },
      10.seconds
    )

    logger.info("Try loading items by keys")
    val firstForum  = sampleForums.head
    val firstThread = sampleThreads.head
    val firstReply  = sampleReplies.head
    Await.result(
      for {
        Some(forum)  <- mapper.loadByKey[Forum](firstForum.name)
        Some(thread) <- mapper.loadByKey[ForumThread](firstThread.forumName, firstThread.subject)
        forums  <- mapper.batchLoadByKeys[Forum](sampleForums.map(_.name))
        threads <- mapper.batchLoadByKeys[ForumThread](sampleThreads.map(_.forumName), sampleThreads.map(_.subject))
      } yield {
        assert {
          forum == firstForum
        }
        assert {
          thread == firstThread
        }
        assert {
          forums.size == sampleForums.size
        }
        assert {
          threads.size == sampleThreads.size
        }
      },
      10.seconds
    )

    logger.info("Try deleting items by keys")
    Await.result(
      for {
        // delete the first forum twice
        Some(oldItem) <- mapper.deleteByKey[Forum](firstForum.name)
        None          <- mapper.deleteByKey[Forum](firstForum.name)
        // delete the first thread
        Some(oldThread) <- mapper.deleteByKey[ForumThread](firstThread.forumName, firstThread.subject)
        // count the tables
        forumCount  <- mapper.countScan[Forum]()
        threadCount <- mapper.countScan[ForumThread]()
        // put the items back in
        _ <- mapper.dump(firstForum)
        _ <- mapper.dump(firstThread)
      } yield {
        assert {
          oldItem == firstForum
        }
        assert {
          forumCount == sampleForums.size - 1
        }
        assert {
          threadCount == sampleThreads.size - 1
        }
      },
      10.seconds
    )

    logger.info("Try deleting objects")
    Await.result(
      for {
        _ <- mapper.delete(firstReply)
        replyCount <- mapper.countScan[Reply]()
        _ <- mapper.dump(firstReply)
      } yield {
        assert {
          replyCount == sampleReplies.size - 1
        }
      },
      10.seconds
    )

    logger.info("Batch delete then batch dump")
    Await.result(
      for {
        _ <- mapper.batchDelete(sampleReplies)
        emptyCount <- mapper.countScan[Reply]()
        _ <- mapper.batchDump(sampleReplies)
        replyCount <- mapper.countScan[Reply]()
      } yield {
        assert {
          emptyCount == 0
        }
        assert {
          replyCount == sampleReplies.size
        }
      },
      10.seconds
    )

    logger.info("Batch delete by keys then batch dump")
    Await.result(
      for {
        _ <- mapper.batchDeleteByKeys[Forum](sampleForums.map(_.name))
        _ <- mapper.batchDeleteByKeys[ForumThread](sampleThreads.map(_.forumName), sampleThreads.map(_.subject))
        emptyForumCount  <- mapper.countScan[Forum]()
        emptyThreadCount <- mapper.countScan[ForumThread]()
        _ <- mapper.batchDump(sampleForums)
        _ <- mapper.batchDump(sampleThreads)
        forumCount  <- mapper.countScan[Forum]()
        threadCount <- mapper.countScan[ForumThread]()
      } yield {
        assert {
          emptyForumCount == 0
        }
        assert {
          emptyThreadCount == 0
        }
        assert {
          forumCount == sampleForums.size
        }
        assert {
          threadCount == sampleThreads.size
        }
      },
      10.seconds
    )

    client.shutdown()
  }
}


object TestSingleThreadedBatchWriter {

  val tableName = "load-test"

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val source = new Iterator[WriteRequest] {
      override def hasNext = true
      override def next =
        new WriteRequest()
          .withPutRequest(
              new PutRequest()
                .withItem(
                    Map(
                      "key" ->
                        new AttributeValue(UUID.randomUUID.toString),
                      "value" ->
                        new AttributeValue(UUID.randomUUID.toString)
                    ).asJava
                  )
            )
    }

    val credentials = new PropertiesCredentials(
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("credentials.properties")
      )

    val loader = new SingleThreadedBatchWriter(tableName, credentials)

    loader.run(source)

  }

}

object TestConcurrentBatchWriter {

  val tableName = "load-test"

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    def genWriteRequests(total: Int) = new Iterable[WriteRequest] {
      override def iterator: Iterator[WriteRequest] =
        new Iterator[WriteRequest] {
          var i = 0
          override def hasNext = i < total
          override def next = {
            i += 1
            new WriteRequest()
              .withPutRequest(
                  new PutRequest()
                    .withItem(
                        Map(
                          "key" ->
                            new AttributeValue(UUID.randomUUID.toString),
                          "value" ->
                            new AttributeValue(UUID.randomUUID.toString)
                        ).asJava
                      )
                )
          }
      }
    }

    val credentials = new PropertiesCredentials(
        this.getClass()
            .getClassLoader()
            .getResourceAsStream("credentials.properties")
      )

    val batchWriter = new ConcurrentBatchWriter(tableName, credentials, 5)

    val errorQueue = new java.util.concurrent.ConcurrentLinkedQueue[FailedBatch[String]]

    val writer = batchWriter.createWriteGroup("my-meta-data", errorQueue)

    writer.queueWriteRequests(genWriteRequests(1000000))

    val errorFree = writer.awaitCompletionOfAllWrites()

    if (!errorFree) {
      for (e <- errorQueue.iterator.asScala) {
        logger.error(e.tableName, e.cause)
      }
    }

    batchWriter.shutdownAndAwaitTermination()

  }

}

