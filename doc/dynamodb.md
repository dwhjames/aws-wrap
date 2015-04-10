---
layout: default
title: aws-wrap
---

# Getting started with DynamoDB

## Case class mapping for DynamoDB

A model. A forum has a name and category, and counts of the number of threads,
messages, and views.

{% highlight scala %}
import com.github.dwhjames.awswrap.dynamodb._
case class Forum(
    name:     String,
    category: String,
    threads:  Long,
    messages: Long,
    views:    Long
)
{% endhighlight %}

In the companion object we’ll track information about the corresponding
DynamoDB table. First, we statically store the table name, the attribute
names, and the table creation request object.

{% highlight scala %}
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
{% endhighlight %}

Second, we define an implicit [serializer]({{site.baseurl}}/api/v{{site.latestrelease}}/index.html#com.github.dwhjames.awswrap.dynamodb.DynamoDBSerializer)
that will convert between our `Forum` type and a mapping of attribute names as
`String` and values as `AttributeValue`.

{% highlight scala %}
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
      item: collection.mutable.Map[String, AttributeValue]) =
      Forum(
        name     = item(Attributes.name),
        category = item(Attributes.category),
        threads  = item(Attributes.threads),
        messages = item(Attributes.messages),
        views    = item(Attributes.views)
      )
  }
}
{% endhighlight %}

The dynamodb [package object]({{site.baseurl}}/api/v{{site.latestrelease}}/index.html#com.github.dwhjames.awswrap.dynamodb.package)
contains implicit views between Scala types and `AttributeValue`, which makes
the definitions of `toAttributeMap` and `fromAttributeMap` clean (and maybe a
little magical).

Let’s construct a couple of sample forum objects.

{% highlight scala %}
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
{% endhighlight %}

Now let’s create a client to access DynamoDB: the raw SDK client, and our
wrapping client.

{% highlight scala %}
val sdkClient = new AmazonDynamoDBAsyncClient(myCredentials)
val client    = new AmazonDynamoDBScalaClient(sdkClient)
{% endhighlight %}

And then in turn wrap that client in the mapper.

{% highlight scala %}
val mapper = AmazonDynamoDBScalaMapper(client)
{% endhighlight %}

Finally, assuming we have a live table, we can demonstrate the mapper with a
few example operations.

{% highlight scala %}
Await.result(
  for {
{% endhighlight %}

We can initialize the table by loading all our sample objects as items into the
table, using [batchDump]({{site.baseurl}}/api/v{{site.latestrelease}}/index.html#com.github.dwhjames.awswrap.dynamodb.AmazonDynamoDBScalaMapper).

{% highlight scala %}
    _ <- mapper.batchDump(sampleForums)
{% endhighlight %}

We can count the number of items in the table, using [countScan]({{site.baseurl}}/api/v{{site.latestrelease}}/index.html#com.github.dwhjames.awswrap.dynamodb.AmazonDynamoDBScalaMapper).

{% highlight scala %}
    forumCount <- mapper.countScan[Forum]()
{% endhighlight %}

We can scan for items in the table, using [scan]({{site.baseurl}}/api/v{{site.latestrelease}}/index.html#com.github.dwhjames.awswrap.dynamodb.AmazonDynamoDBScalaMapper) with a condition.

{% highlight scala %}
    forums <- mapper.scan[Forum](
                Map("Category" ->
                      ScanCondition.contains("Services")))
{% endhighlight %}

And we can find a forum item in the table by its hash key, using [loadByKey]({{site.baseurl}}/api/v{{site.latestrelease}}/index.html#com.github.dwhjames.awswrap.dynamodb.AmazonDynamoDBScalaMapper).

{% highlight scala %}
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
{% endhighlight %}

… and checking that these return the expected results.
