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

  val recipeForm: Form[Recipe] = Form(
    mapping(
      "id" -> nonEmptyText,
      "name" -> optional(text)
    )(Recipe.apply)(Recipe.unapply)

  )

  def index = Action {
    Async {
      Recipe.all().map {
        case Result(_, recipes) => Ok(views.html.index(recipes))
        case Errors(Seq(AWSError(DDBErrors.RESOURCE_NOT_FOUND_EXCEPTION, _))) => Redirect(routes.Application.notready())
        case Errors(errors) => InternalServerError(views.html.error(errors))
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
        case Errors(errors) => InternalServerError(views.html.error(errors))
      }
    }
  }

  def show(id: String) = Action {
    Async {
      Recipe.byId(id).map {
        case Result(_, recipe: Recipe) => Ok(recipe.name.getOrElse("noname"))
        case Errors(Seq(AWSError(DDBErrors.RESOURCE_NOT_FOUND_EXCEPTION, _))) => NotFound("Recipe not found")
        case Errors(errors) => InternalServerError(views.html.error(errors))
      }
    }
  }

  def create = Action {
    Ok(views.html.create(recipeForm))
  }

  def createPost = Action { implicit request =>
    recipeForm.bindFromRequest.fold(
      errors => BadRequest(views.html.create(errors)),
      recipe => {
        Async {
          Recipe.insert(recipe).map {
            case Result(_, _) => Redirect(routes.Application.index())
            case Errors(errors) => InternalServerError(views.html.error(errors))
          }
        }
      }
    )
  }

}
