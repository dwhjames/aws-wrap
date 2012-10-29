package aws.core

import play.api.libs.ws._
import play.api.libs.ws.WS._

import java.util.Date
import java.text.SimpleDateFormat

import com.ning.http.client.Realm.{ AuthScheme, RealmBuilder }

object AWS {
  val awsVersion = "2009-04-15"
  lazy val key: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(0)
  lazy val secret: String = scala.io.Source.fromFile(System.getProperty("user.home") + "/.awssecret").getLines.toList(1)

  def isoDateFormat(date: Date) = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date)

  def isoBasicFormat(date: Date) = {
    val iso = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    iso.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
    iso.format(date)
  }

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
