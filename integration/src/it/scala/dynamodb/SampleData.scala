package com.pellucid.wrap.dynamodb

import org.joda.time.DateTime


object SampleData {

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
}
