
package aws.wrap

import dynamodb._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._

object UserHomeCredentialsProvider extends AWSCredentialsProvider {

  override lazy val getCredentials: AWSCredentials =
    try {
      val lines = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList
      new BasicAWSCredentials(lines(0), lines(1))
    } catch {
      case (t: Throwable) =>
        throw new AmazonClientException("No credentials were found at /.awssecret", t)
    }

  override def refresh() {}
}

object Scratch {

  def main(args: Array[String]) {
    val client = AmazonDynamoDBScalaClient.fromAsyncClient(new AmazonDynamoDBAsyncClient(UserHomeCredentialsProvider))

    val tableName = "test-table"

    def awaitTableCreation(): TableDescription = {
      val result = Await.result(
        client.describeTable(tableName),
        Duration(10, SECONDS)
      )
      val description = result.getTable
      if (description.getTableStatus != "ACTIVE") {
        Thread.sleep(1000)
        awaitTableCreation()
      } else description
    }

    Await.result(
      client.createTable(
        tableName = tableName,
        provisionedThroughput = Schema.provisionedThroughput(10L, 10L),
        attributeDefinitions = Seq(
          Schema.stringAttribute("ForumName"),
          Schema.stringAttribute("Subject"),
          Schema.stringAttribute("LastPostDateTime")
        ),
        keySchema = Seq(
          Schema.hashKey("ForumName"),
          Schema.rangeKey("Subject")
        ),
        localSecondaryIndexes = Seq(
          new LocalSecondaryIndex()
            .withIndexName("LastPostIndex")
            .withKeySchema(
              Schema.hashKey("ForumName"),
              Schema.rangeKey("LastPostDateTime")
            )
            .withProjection(
              new Projection()
                .withProjectionType(ProjectionType.KEYS_ONLY)
            )
        )
      ),
      Duration(10, SECONDS)
    )

    println(awaitTableCreation())

    client.shutdown()
  }
}
