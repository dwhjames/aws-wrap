package aws.core

import scala.util.{Try, Success, Failure}
import play.api.libs.ws.{Response => WSResponse}

case class Result(requestId: String, boxUsage: String)

//sealed trait Error extends Response

case class Error(requestId: String, errors: Seq[(String, String)]) extends Exception

//case class NetworkError extends Response


object Result {

  def fromWS(wsresp: WSResponse): Try[Result] = wsresp.status match {
    case 200 => Success(Result(wsresp.xml \\ "RequestId" text, wsresp.xml \\ "BoxUsage" text))
    case _ => {
      val errors = wsresp.xml \\ "Error" map { node =>
        (node \ "Code" text) -> (node \ "Message" text)
      }
      Failure(Error(wsresp.xml \\ "RequestID" text, errors))
    }
  }

}

