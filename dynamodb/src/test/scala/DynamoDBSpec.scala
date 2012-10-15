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
      val status = Await.result(DynamoDB.describeTable(tableName), Duration(30, SECONDS)).body.status
      status == Status.CREATING || status == Status.UPDATING
    }) {
      Thread.sleep(2000)
    }
    Thread.sleep(2000)
  }

  val provisioned = ProvisionedThroughput(10L, 10L)

  "DynamoDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "List tables" in {
      val r = Await.result(DynamoDB.listTables(), Duration(30, SECONDS))
    }

    "Create and delete tables" in {
      val schema = PrimaryKey(StringKey("id"))
      Await.result(DynamoDB.createTable("create-table-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }
      // Loop until the table is ready
      waitUntilReady("create-table-test")
      ensureSuccess(Await.result(DynamoDB.deleteTable("create-table-test"), Duration(30, SECONDS)))
    }

    "Update a table" in {
      val schema = PrimaryKey(StringKey("id"))
      val newProvisioned = ProvisionedThroughput(15L, 15L)
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
      val schema = PrimaryKey(StringKey("id"))
      val item: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("ntesla"),
        "firstName" -> DDBString("Nikola"),
        "lastName" -> DDBString("Tesla"),
        "awesomeLevel" -> DDBNumber(1000)
      )
      val key = KeyValue("ntesla")
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

      // Update it
      ensureSuccess(Await.result(
        DynamoDB.updateItem("put-item-test", key, Map("firstName" -> Update(DDBString("Nico"), UpdateAction.PUT))),
        Duration(30, SECONDS)
      ))

      // Check that the update was effective
      Await.result(DynamoDB.getItem("put-item-test", key, Seq("firstName"), true), Duration(30, SECONDS)) match {
        case Result(_, body) => body.attributes.get("firstName") should be equalTo(Some(DDBString("Nico")))
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

    "Do a query" in {
      val schema = PrimaryKey(StringKey("id"), StringKey("lastName"))
      val item: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("ntesla"),
        "firstName" -> DDBString("Nikola"),
        "lastName" -> DDBString("Tesla"),
        "awesomeLevel" -> DDBNumber(1000)
      )
      val key = KeyValue("ntesla")
      // Create a table
      Await.result(DynamoDB.createTable("query-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }

      // Loop until the table is ready
      waitUntilReady("query-test")

      // Put the item
      ensureSuccess(Await.result(DynamoDB.putItem("query-test", item), Duration(30, SECONDS)))

      // Try a query
      val q = Query("query-test", DDBString("ntesla"))
      Await.result(DynamoDB.query(q), Duration(30, SECONDS)) match {
        case Result(_, body) => body.items should be equalTo(Seq(item))
        case Errors(errors) => failure(errors.toString)
      }

      // Delete the table
      ensureSuccess(Await.result(DynamoDB.deleteTable("query-test"), Duration(30, SECONDS)))
    }

    "Do a scan" in {
      val schema = PrimaryKey(StringKey("id"), StringKey("lastName"))
      val item: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("ntesla"),
        "firstName" -> DDBString("Nikola"),
        "lastName" -> DDBString("Tesla"),
        "awesomeLevel" -> DDBNumber(1000)
      )
      val key = KeyValue("ntesla")
      // Create a table
      Await.result(DynamoDB.createTable("scan-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }

      // Loop until the table is ready
      waitUntilReady("scan-test")

      // Put the item
      ensureSuccess(Await.result(DynamoDB.putItem("scan-test", item), Duration(30, SECONDS)))

      // Try a scan
      Await.result(DynamoDB.scan("scan-test"), Duration(30, SECONDS)) match {
        case Result(_, body) => body.items should be equalTo(Seq(item))
        case Errors(errors) => failure(errors.toString)
      }

      // Delete the table
      ensureSuccess(Await.result(DynamoDB.deleteTable("scan-test"), Duration(30, SECONDS)))
    }

    "Batch operations" in {
      val schema = PrimaryKey(StringKey("id"))
      val niko: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("ntesla"),
        "firstName" -> DDBString("Nikola"),
        "lastName" -> DDBString("Tesla"),
        "awesomeLevel" -> DDBNumber(1000)
      )
      val tom: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("tedison"),
        "firstName" -> DDBString("Thomas"),
        "lastName" -> DDBString("Edison"),
        "awesomeLevel" -> DDBNumber(800)
      )
      // Create a table
      Await.result(DynamoDB.createTable("batch-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }

      // Loop until the table is ready
      waitUntilReady("batch-test")

      // Add items as a batch
      val batchWrite = Map(
        "batch-test" -> Seq(
          PutRequest(niko),
          PutRequest(tom)
        )
      )
      ensureSuccess(Await.result(DynamoDB.batchWriteItem(batchWrite), Duration(30, SECONDS)))

      // Get items as a batch
      val batchGet = Map(
        "batch-test" -> GetRequest(Seq(KeyValue("tedison")))
      )
      Await.result(DynamoDB.batchGetItem(batchGet), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, response) => {
          println("Success = " + success)
          success
        }
      }


      // Delete the table
      ensureSuccess(Await.result(DynamoDB.deleteTable("batch-test"), Duration(30, SECONDS)))
    }

  }
}