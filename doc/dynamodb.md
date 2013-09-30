---
layout: default
title: aws-wrap by Pellucid Analytics
---

# Getting started with DynamoDB

## Case class mapping for DynamoDB

A model. A forum has a name and category, and counts of the number of threads,
messages, and views.

```scala
case class Forum(
    name:     String,
    category: String,
    threads:  Long,
    messages: Long,
    views:    Long
)
```

In the companion object we’ll track information about the corresponding
DynamoDB table. First, we statically store the table name, the attribute
names, and the table creation request object.

```scala
object Forum {

  val tableName = "Forum"

  object Attributes {
    val name     = "Name"
    val category = "Category"
    val threads  = "Threads"
    val messages = "Messages"
    val views    = "Views"
  }

  val tableRequest =
    new CreateTableRequest()
      .withTableName(Forum.tableName)
      .withProvisionedThroughput(
        Schema.provisionedThroughput(10L, 5L))
      .withAttributeDefinitions(
        Schema.stringAttribute(Attributes.name))
      .withKeySchema(
        Schema.hashKey(Attributes.name))
```

Second, we define an implicit [serializer]({{site.baseurl}}/api/current/#com.pellucid.wrap.dynamodb.DynamoDBSerializer)
that will convert between our `Forum` type and a mapping of attribute names as
`String` and values as `AttributeValue`.

```scala
  implicit object forumSerializer
    extends DynamoDBSerializer[Forum] {

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

    override def fromAttributeMap(
      item: mutable.Map[String, AttributeValue]) =
      Forum(
        name     = item(Attributes.name),
        category = item(Attributes.category),
        threads  = item(Attributes.threads),
        messages = item(Attributes.messages),
        views    = item(Attributes.views)
      )
  }
}
```

The dynamodb [package object]({{site.baseurl}}/api/current/#com.pellucid.wrap.dynamodb.package)
contains implicit views between Scala types and `AttributeValue`, which makes
the definitions of `toAttributeMap` and `fromAttributeMap` clean (and maybe a
little magical).

Let’s construct a couple of sample forum objects.

```scala
val sampleForums = Seq(
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
```

Now let’s create a client to access DynamoDB: the raw SDK client, and our
wrapping client.

```scala
val sdkClient = new AmazonDynamoDBAsyncClient(myCredentials)
val client    = new AmazonDynamoDBScalaClient(sdkClient)
```

And then in turn wrap that client in the mapper.

```scala
val mapper = AmazonDynamoDBScalaMapper(client)
```

Finally, assuming we have a live table, we can demonstrate the mapper with a
few example operations.

```scala
Await.result(
  for {
```

We can initialize the table by loading all our sample objects as items into the
table, using [batchDump]({{site.baseurl}}/api/current/index.html#com.pellucid.wrap.dynamodb.AmazonDynamoDBScalaMapper).

```scala
    _ <- mapper.batchDump(sampleForums)
```

We can count the number of items in the table, using [countScan]({{site.baseurl}}/api/current/index.html#com.pellucid.wrap.dynamodb.AmazonDynamoDBScalaMapper).

```scala
    forumCount <- mapper.countScan[Forum]()
```

We can scan for items in the table, using [scan]({{site.baseurl}}/api/current/index.html#com.pellucid.wrap.dynamodb.AmazonDynamoDBScalaMapper) with a condition.

```scala
    forums <- mapper.scan[Forum](
                Map("Category" ->
                      ScanCondition.contains("Services")))
```

And we can find a forum item in the table by its hash key, using [loadByKey]({{site.baseurl}}/api/current/index.html#com.pellucid.wrap.dynamodb.AmazonDynamoDBScalaMapper).

```scala
    Some(forum) <- mapper.loadByKey[Forum](sampleForums.head.name)
  } yield {
    assert {
      forumCount == sampleForums.size
    }
    assert {
      forums.size == sampleForums.size
    }
    assert {
      forum == sampleForums.head
    }
  },
  10.seconds
)
```

… and checking that these return the expected results.
