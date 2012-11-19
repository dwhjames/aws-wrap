package controllers

import play.api._
import play.api.mvc._
import play.api.data._
import play.api.data.Forms._

import aws.core._
import aws.core.parsers._
import aws.dynamodb._

import _root_.models._

import scala.concurrent.ExecutionContext.Implicits.global

object Application extends Controller {

  val createRecipeForm: Form[Recipe] = Form(
    mapping(
      "name" -> optional(text),
      "description" -> optional(text)
    )((name, description) =>
      Recipe(Item.randomUUID, name, description)
    )(recipe => (Some(recipe.name, recipe.description)))
  )

  def index = Action { implicit request =>
    val success = request.flash.get("success")
    val error = request.flash.get("error")
    Async {
      Recipe.all().map {
        case Result(_, recipes) => Ok(views.html.index(recipes, success, error))
        case AWSError(_, DDBErrors.RESOURCE_NOT_FOUND_EXCEPTION, _) => Redirect(routes.Application.notready())
        case err@AWSError(_, _, _) => InternalServerError(views.html.error(err))
      }
    }
  }

  def notready() = Action {
    Ok(views.html.notready())
  }

  def init = Action {
    Async {
      Recipe.initialize().map {
        case Result(_, table) => Redirect(routes.Application.index())
        case err@AWSError(_, _, _) => InternalServerError(views.html.error(err))
      }
    }
  }

  def show(id: String) = Action {
    Async {
      Recipe.byId(id).map {
        case Result(_, recipe: Recipe) => Ok(views.html.show(recipe))
        case AWSError(_, DDBErrors.RESOURCE_NOT_FOUND_EXCEPTION, _) => NotFound("Recipe not found")
        case err@AWSError(_, _, _) => InternalServerError(views.html.error(err))
      }
    }
  }

  def delete(id: String) = Action {
    Async {
      Recipe.delete(id).map {
        case Result(_, _) => Redirect(routes.Application.index).flashing("success" -> "Item successfully deleted")
        case AWSError(_, code, message) => Redirect(routes.Application.index).flashing("error" -> ("Error deleting: " + message))
      }
    }
  }

  def create = Action {
    Ok(views.html.create(createRecipeForm))
  }

  def createPost = Action { implicit request =>
    createRecipeForm.bindFromRequest.fold(
      errors => BadRequest(views.html.create(errors)),
      recipe => {
        Async {
          Recipe.insert(recipe).map {
            case Result(_, _) => Redirect(routes.Application.index()).flashing("success" -> "Recipe created")
            case err@AWSError(_, _, _) => InternalServerError(views.html.error(err))
          }
        }
      }
    )
  }

}
