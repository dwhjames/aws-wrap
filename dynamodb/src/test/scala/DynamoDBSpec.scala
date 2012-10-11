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
      Await.result(DynamoDB.createTable("create-table-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }
      // Loop until the table is ready
      while(Await.result(DynamoDB.describeTable("create-table-test"), Duration(30, SECONDS)).body.status == Status.CREATING) ()
      Await.result(DynamoDB.deleteTable("create-table-test"), Duration(30, SECONDS))
      success
    }

    "Put and delete items" in {
      val schema = KeySchema(KeySchemaElement("id", DDBString))
      val item: Map[String, DDBAttribute] = Map(
        "id" -> DDBString("ntesla"),
        "firstName" -> DDBString("Nikola"),
        "lastName" -> DDBString("Tesla"),
        "awesomeLevel" -> DDBNumber(1000)
      )
      val provisioned = ProvisionedThroughput(5L, 5L)
      // Create a table
      Await.result(DynamoDB.createTable("put-item-test", schema, provisioned), Duration(30, SECONDS)) match {
        case Errors(errors) => failure(errors.toString)
        case Result(_, description) => description.status should be equalTo(Status.CREATING)
      }

      // Loop until the table is ready
      while(Await.result(DynamoDB.describeTable("put-item-test"), Duration(30, SECONDS)).body.status == Status.CREATING) ()

      // Put the item
      Await.result(DynamoDB.putItem("put-item-test", item), Duration(30, SECONDS)) match {
        case Result(_, response) => success
        case Errors(errors) => failure(errors.toString)
      }

      // Delete it
      Await.result(DynamoDB.deleteItem("put-item-test", Key(DDBString("ntesla"))), Duration(30, SECONDS)) match {
        case Result(_, response) => success
        case Errors(errors) => failure(errors.toString)
      }

      // Delete the table
      Await.result(DynamoDB.deleteTable("put-item-test"), Duration(30, SECONDS))
      success
    }

  }
}