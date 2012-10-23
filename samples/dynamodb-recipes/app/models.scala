package models

import scala.concurrent.Future

import aws.core._
import aws.core.Types._
import aws.dynamodb._
import aws.dynamodb.DDBAttribute._

import aws.dynamodb.DDBRegion.DEFAULT

import scala.concurrent.ExecutionContext.Implicits.global

case class Recipe(
  id: String,
  name: Option[String]
) {

  def asItem = Item.build(
    "id" -> DDBString(id)
  ) ++ name.map { n =>
    Item.build("name" -> DDBString(n))
  }.getOrElse(Item.empty)

}

object Recipe {

  // DynamoDB models
  val TABLE_NAME = "recipe_recipe"
  val key = PrimaryKey(StringKey("id"))

  def initialize(): Future[SimpleResult[TableDescription]] = {
    DynamoDB.createTable(TABLE_NAME, key, ProvisionedThroughput(5L, 5L))
  }

  def insert(recipe: Recipe): Future[SimpleResult[ItemResponse]] = {
    DynamoDB.putItem(TABLE_NAME, recipe.asItem)
  }

  def byId(id: String): Future[SimpleResult[Recipe]] = {
    DynamoDB.getItem(TABLE_NAME, KeyValue(id)).map { response =>
      response.flatMap(itemResponse => {
        fromAWS(itemResponse.item).map { recipe =>
          Result(EmptyMeta, recipe)
        }.getOrElse(
          Errors(EmptyMeta, Seq(AWSError(DDBErrors.RESOURCE_NOT_FOUND_EXCEPTION, "")))
        )
      })
    }
  }

  def all(): Future[SimpleResult[Seq[Recipe]]] = {
    DynamoDB.scan(TABLE_NAME).map { response =>
      response.map { queryResponse =>
        queryResponse.items.flatMap(fromAWS(_))
      }
    }
  }

  def fromAWS(item: Item): Option[Recipe] = {
    val idOpt = item.get("id").flatMap(_.asOpt[String])
    val name = item.get("name").flatMap(_.asOpt[String])
    idOpt.map { id => Recipe(id, name) }
  }

}


