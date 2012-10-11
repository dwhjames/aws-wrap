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

  import aws.core._
  import aws.core.Types._

  implicit val region = DDBRegion.EU_WEST_1

  def ensureSuccess[T](r: SimpleResult[T]) = r match {
    case Result(_, _) => success
    case Errors(errors) => failure(errors.toString)
  }

  def waitUntilReady(tableName: String) {
    while({
      val status = Await.result(DynamoDB.describeTable("update-table-test"), Duration(30, SECONDS)).body.status
      status == Status.CREATING || status == Status.UPDATING
    }) {
      Thread.sleep(1000)
    }
  }

  "DynamoDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "List tables" in {
      val r = Await.result(DynamoDB.listTables(), Duration(30, SECONDS))
    }

    "Create and delete tables" in {
      val schema = KeySchema(KeySchemaElement("id", DDBString))
      val provisioned = ProvisionedThroughput(5L, 5L)
      Await.result(DynamoDB.createTable("create-table-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }
      // Loop until the table is ready
      waitUntilReady("create-table-test")
      ensureSuccess(Await.result(DynamoDB.deleteTable("create-table-test"), Duration(30, SECONDS)))
    }

    "Update a table" in {
      val schema = KeySchema(KeySchemaElement("id", DDBString))
      val provisioned = ProvisionedThroughput(5L, 5L)
      val newProvisioned = ProvisionedThroughput(10L, 10L)
      Await.result(DynamoDB.createTable("update-table-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }
      // Loop until the table is ready
      waitUntilReady("update-table-test")
      // Update it
      ensureSuccess(Await.result(DynamoDB.updateTable("update-table-test", newProvisioned), Duration(30, SECONDS)))
      waitUntilReady("update-table-test")
      ensureSuccess(Await.result(DynamoDB.deleteTable("update-table-test"), Duration(30, SECONDS)))
    }

    "Put and delete items" in {
      val schema = KeySchema(KeySchemaElement("id", DDBString))
      val item: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("ntesla"),
        "firstName" -> DDBString("Nikola"),
        "lastName" -> DDBString("Tesla"),
        "awesomeLevel" -> DDBNumber(1000)
      )
      val key = Key(DDBString("ntesla"))
      val provisioned = ProvisionedThroughput(5L, 5L)
      // Create a table
      Await.result(DynamoDB.createTable("put-item-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }

      // Loop until the table is ready
      waitUntilReady("put-item-test")

      // Put the item
      ensureSuccess(Await.result(DynamoDB.putItem("put-item-test", item), Duration(30, SECONDS)))

      // Check that it's there
      Await.result(DynamoDB.getItem("put-item-test", key, Seq("firstName"), true), Duration(30, SECONDS)) match {
        case Result(_, body) => body.attributes.get("firstName") should be equalTo(Some(DDBString("Nikola")))
        case Errors(errors) => failure(errors.toString)
      }

      // Delete it
      ensureSuccess(Await.result(DynamoDB.deleteItem("put-item-test", key), Duration(30, SECONDS)))

      // Check that it's gone
      Await.result(DynamoDB.getItem("put-item-test", key, Seq("firstName"), true), Duration(30, SECONDS)) match {
        case Result(_, body) => body.attributes.get("firstName") should be equalTo(None)
        case Errors(errors) => failure(errors.toString)
      }

      // Delete the table
      ensureSuccess(Await.result(DynamoDB.deleteTable("put-item-test"), Duration(30, SECONDS)))
    }

  }
}