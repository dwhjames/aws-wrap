package aws.core

import play.api.libs.ws._
import play.api.libs.ws.WS._

import java.util.Date
import java.text.SimpleDateFormat

import com.ning.http.client.Realm.{ AuthScheme, RealmBuilder }

object AWS {
  /**
   * The current AWS key, read from the first line of `~/.awssecret`
   */
  lazy val key: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(0)

  /**
   * The current AWS secret, read from the second line of `~/.awssecret`
   */
  lazy val secret: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(1)

  /**
   * Format the date in ISO: `yyyy-MM-dd'T'HH:mm:ssZ`
   */
  def isoDateFormat(date: Date) = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date)

  /**
   * Format the date in ISO basic, in GMT: `yyyyMMdd'T'HHmmss'Z`
   */
  def isoBasicFormat(date: Date) = {
    val iso = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    iso.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
    iso.format(date)
  }

  def httpDateFormat(date: Date) = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z").format(date)
  def httpDateparse(date: String) = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z").parse(date)

  object Parameters {
    def TimeStamp(date: Date) = "Timestamp" -> isoDateFormat(date)
    def Expires(seconds: Long) = "Expires" -> {
      val now = new Date().getTime()
      (now / 1000 + seconds).toString
    }
    def Action(a: String) = ("Action" -> a)
    def AWSAccessKeyId(key: String) = ("AWSAccessKeyId" -> AWS.key)
    def Version(v: String) = ("Version" -> v)
    def SignatureVersion(v: String) = ("SignatureVersion" -> v)
    def SignatureMethod(m: String) = ("SignatureMethod" -> m)
  }
}
