package aws.ses

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.WS._
import aws.core._

import org.specs2.mutable._

object TestUtils extends Specification { // Evil hack to access Failure

  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import java.util.concurrent.TimeUnit._

  implicit val region = SESRegion.US_EAST_1

  def checkResult[M <: Metadata, T](r: Result[M, T]) = r match {
    case AWSError(code, message) => failure(message)
    case Result(_, _) => success
  }

  def waitFor[T](f: Future[T]) = Await.result(f, Duration(30, SECONDS))

}

object SESSpec extends Specification {

  import TestUtils._

  "SimpleDB API" should {
    import scala.concurrent.ExecutionContext.Implicits.global

    "Send Emails" in {
      val mail = Email(
        subject = "Hello",
        body = "Hello World",
        contentType = ContentTypes.PLAIN_TEXT,
        source = "jto+ses@zenexity.com",
        replyTo = "jto+ses@zenexity.com" :: Simulators.SUCCESS :: Nil,
        destinations = To(Simulators.SUCCESS) :: BCC(Simulators.SUCCESS) :: Nil
      )

      val r = waitFor(SES.send(mail))
      checkResult(r)
    }


    "Send raw Email" in {
      val message =
          s"""From: "Meh" <jto+ses@zenexity.com>
          |To: "Simulator" <${Simulators.SUCCESS}>
          |Date: Fri, 17 Dec 2010 14:26:21 -0800
          |Subject: Hello
          |Message-ID: <61967230-7A45-4A9D-BEC9-87CBCF2211C9@example.com>
          |Accept-Language: en-US
          |Content-Language: en-US
          |Content-Type: text/plain; charset="utf-8"
          |Content-Transfer-Encoding: quoted-printable
          |MIME-Version: 1.0
          |
          |Hello, I hope you are having a good day.
          |
          |- Meh""".stripMargin

      val r = waitFor(SES.sendRaw(message))
      checkResult(r)
    }
  }
}