package aws.dynamodb

import scala.util.{Try, Success, Failure}
import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._
import aws.dynamodb.models._

import org.specs2.mutable._

object DynamoDBSpec extends Specification {

  import scala.concurrent._
  import scala.concurrent.util._
  import java.util.concurrent.TimeUnit._

  implicit val region = DDBRegion.EU_WEST_1

  "DynamoDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "List tables" in {
      val r = Await.result(DynamoDB.listTables(), Duration(30, SECONDS))
    }

    "Create and delete tables" in {
      val schema = KeySchema(KeySchemaElement("id", DDBString))
      val provisioned = ProvisionedThroughput(5L, 5L)
      val r = Await.result(DynamoDB.createTable("create-table-test", schema, provisioned), Duration(30, SECONDS))
      r.body.status should be equalTo(Status.CREATING)
      // Loop until the table is ready
      while(Await.result(DynamoDB.describeTable("create-table-test"), Duration(30, SECONDS)).body.status == Status.CREATING) ()
      val r3 = Await.result(DynamoDB.deleteTable("create-table-test"), Duration(30, SECONDS))
      success
    }

  }
}